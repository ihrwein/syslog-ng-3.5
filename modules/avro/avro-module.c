/*
 * Copyright (c) 2013 BalaBit IT Ltd, Budapest, Hungary
 * Copyright (c) 2013 Tibor Benke <btibi@balabit.hu>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * As an additional exemption you are allowed to compile & link against the
 * OpenSSL libraries as published by the OpenSSL project. See the file
 * COPYING for details.
 *
 */


#include "avro-module-parser.h"
#include "avro-module.h"
#include "plugin.h"
#include "messages.h"
#include "misc.h"
#include "stats.h"
#include "logqueue.h"
#include "plugin-types.h"
#include "logthrdestdrv.h"
#include "value-pairs.h"
#include "vptransform.h"
#include <signal.h>
#include <assert.h>

typedef struct
{
  LogThrDestDriver super;
  avro_schema_t schema;
  avro_value_iface_t *writer_iface;
  avro_file_writer_t data_file_writer;
  LogTemplateOptions template_options;
  LogTemplate *file_name;
  GString *current_file_name;
  GString *active_file_name;
  LogTemplate* timestamp;
  GString* current_timestamp;
  gint32 seq_num;

  ValuePairs *vp;
} AvroDriver;

/*
 * Called by YACC
 */

void
avro_mod_dd_set_destination_file(LogDriver *d, LogTemplate *filename)
{
  AvroDriver *self = (AvroDriver *)d;

  log_template_unref(self->file_name);
  self->file_name = log_template_ref(filename);
}

void
avro_mod_dd_set_timestamp(LogDriver *d, LogTemplate *stamp)
{
  AvroDriver *self = (AvroDriver *)d;

  log_template_unref(self->timestamp);
  self->timestamp = log_template_ref(stamp);
}

void
avro_mod_dd_set_value_pairs(LogDriver *d, ValuePairs *vp)
{
  AvroDriver *self = (AvroDriver *)d;

  if (self->vp)
    value_pairs_free(self->vp);
  self->vp = vp;
}

LogTemplateOptions *
avro_mod_dd_get_template_options(LogDriver *s)
{
  AvroDriver *self = (AvroDriver*) s;
  return &self->template_options;
}

static inline gint
assert_zero(AvroDriver *self, gint result)
{
  if (result != 0)
    msg_error("Avro error", evt_tag_str("error", avro_strerror()),
              evt_tag_str("driver", self->super.super.super.id), NULL);
  return result;
}

static inline void*
assert_not_null(AvroDriver *self, void *result)
{
  if (result == NULL)
    msg_error("Avro error", evt_tag_str("error", avro_strerror()),
              evt_tag_str("driver", self->super.super.super.id), NULL);

  return result;
}

static int
get_file_length(char *file_name)
{
  FILE *f = fopen(file_name, "rb");
  if (!f)
    return -1;

  fseek(f, 0L, SEEK_END);
  int size = ftell(f);
  fclose(f);
  return size;
}

static void
fill_json_string_end_with_zeros(char* buffer, int size)
{
  for (int i = size - 1; i >= 0; i--)
    {
      if (buffer[i] != '}')
        buffer[i] = '\0';
      else
        break;
    }
}

static char*
read_content_from_file(char *file_name)
{
  int read_bytes;
  int file_size = get_file_length(file_name);

  if (file_size < 0)
    return NULL;
  char *string = (char*) malloc(file_size * sizeof(char));
  FILE *f = fopen(file_name, "rb");
  read_bytes = fread(string, 1, file_size, f);
  fill_json_string_end_with_zeros(string, file_size);
  assert(read_bytes == file_size);
  fclose(f);
  return string;
}

static gboolean
avro_mod_dd_datafile_open(AvroDriver *self, char *filename)
{
  int error;
  avro_schema_error_t schema_error;
  char *schema_string = read_content_from_file(AVRO_SCHEMA_FILE);
  if (!schema_string)
    {
      msg_error("Unable to open Avro schema file",
                evt_tag_str("driver", self->super.super.super.id),
                evt_tag_str("filename", AVRO_SCHEMA_FILE),
                NULL);
      return FALSE;
    }

  error = assert_zero(self, avro_schema_from_json(schema_string, 0, &self->schema, &schema_error));
  assert(self->schema != NULL);
  assert_not_null(self, self->writer_iface = avro_generic_class_from_schema(self->schema));

  if (access(filename, W_OK) == 0)
    {
      error |= assert_zero(self, avro_file_writer_open(filename, &self->data_file_writer));
    }
  else
    {
      error |= assert_zero(self, avro_file_writer_create(filename, self->schema, &self->data_file_writer));
    }

  free(schema_string);

  if (error)
    {
      msg_error("Unable to open Avro datafile",
                evt_tag_str("driver", self->super.super.super.id),
                evt_tag_str("filename", filename),
                NULL);
      return FALSE;
    }

  msg_verbose("Avro datafile has been opened",
              evt_tag_str("driver", self->super.super.super.id),
              evt_tag_str("filename", filename),
              NULL);

  return (error == 0) ? TRUE : FALSE;
}

static gint
avro_mod_dd_datafile_append_msg(AvroDriver *self, avro_value_t *avro_data)
{
  return assert_zero(self, avro_file_writer_append_value(self->data_file_writer, avro_data));
}

static int
avro_mod_dd_datafile_close(AvroDriver *self)
{
  int error;

  error = assert_zero(self, avro_file_writer_close(self->data_file_writer));
  avro_value_iface_decref(self->writer_iface);
  error = assert_zero(self, avro_schema_decref(self->schema));

  if (error)
    {
      msg_error("Unable to close Avro datafile",
                evt_tag_str("driver", self->super.super.super.id),
                evt_tag_str("filename", self->current_file_name->str),
                NULL);
    }

  msg_debug("Avro datafile closed",
            evt_tag_str("driver", self->super.super.super.id),
            evt_tag_str("filename", self->current_file_name->str),
            NULL);
  return error;
}

static gint
avro_mod_dd_init_msg(AvroDriver *self, avro_value_t *logmsg)
{
  return assert_zero(self, avro_generic_value_new(self->writer_iface, logmsg));
}

static void
avro_mod_dd_free_msg(avro_value_t *logmsg)
{
  return avro_value_decref(logmsg);
}

static gint
avro_mod_dd_set_priority(AvroDriver *self, avro_value_t *parent, LogMessage *logmsg)
{
  int error;
  avro_value_t field;
  avro_value_t branch;

  error = assert_zero(self, avro_value_get_by_name(parent, "PRIORITY", &field, NULL));

  if (logmsg->pri != 0)
    {
      error = assert_zero(self, avro_value_set_branch(&field, VALUE_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_int(&branch, logmsg->pri));
    }
  else
    {
      error = assert_zero(self, avro_value_set_branch(&field, NULL_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_null(&branch));
    }

  return error;
}

static gint
avro_mod_dd_set_stamp(AvroDriver* self, avro_value_t *parent, LogMessage *logmsg)
{
  int error;
  avro_value_t field;
  avro_value_t branch;

  error = assert_zero(self, avro_value_get_by_name(parent, "TIMESTAMP", &field, NULL));
  log_template_format(self->timestamp, logmsg, &self->template_options, LTZ_SEND,
                      self->seq_num, NULL, self->current_timestamp);


  if (self->current_timestamp->len > 0)
    {
      error = assert_zero(self, avro_value_set_branch(&field, VALUE_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_string(&branch, self->current_timestamp->str));
    }
  else
    {
      error = assert_zero(self, avro_value_set_branch(&field, NULL_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_null(&branch));
    }

  return error;
}

static gint
avro_mod_dd_set_string(AvroDriver *self, avro_value_t *parent,
                       LogMessage *logmsg, const char *handle_name,
                       const char *schema_field_name)
{
  int error;
  const gchar *handle_value;
  gssize size;
  avro_value_t field;
  avro_value_t branch;

  error = avro_value_get_by_name(parent, schema_field_name, &field, NULL);

  if (logmsg != NULL ||
      ((handle_value = log_msg_get_value(logmsg, log_msg_get_value_handle(handle_name),
                                         &size)) && !(size > 0)))
    {
      handle_value = log_msg_get_value(logmsg, log_msg_get_value_handle(handle_name), &size);
      error = assert_zero(self, avro_value_set_branch(&field, VALUE_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_string(&branch, handle_value));
    }
  else
    {
      error = assert_zero(self, avro_value_set_branch(&field, NULL_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_null(&branch));
    }

  return error;
}

static gint
avro_mod_dd_set_hostname(AvroDriver* self, avro_value_t *parent, LogMessage *logmsg)
{
  return avro_mod_dd_set_string(self, parent, logmsg, "HOST", "HOSTNAME");
}

static gint
avro_mod_dd_set_app_name(AvroDriver* self, avro_value_t *parent, LogMessage *logmsg)
{
  return avro_mod_dd_set_string(self, parent, logmsg, "PROGRAM", "APP_NAME");
}

static gint
avro_mod_dd_set_procid(AvroDriver* self, avro_value_t *parent, LogMessage *logmsg)
{
  return avro_mod_dd_set_string(self, parent, logmsg, "PID", "PROCID");
}

static gint
avro_mod_dd_set_msgid(AvroDriver* self, avro_value_t *parent, LogMessage *logmsg)
{
  return avro_mod_dd_set_string(self, parent, logmsg, "MSGID", "MSGID");
}

typedef struct
{
  AvroDriver *driver;
  avro_value_t *parent;
  LogMessage *logmsg;
} _NVFunctionUserData;

static gboolean
vp_append_sdata_start(const gchar *name, const gchar *prefix,
                      gpointer *prefix_data, const gchar *prev,
                      gpointer *prev_data, gpointer user_data)
{
  return FALSE;
}

static gboolean
vp_append_sdata_end(const gchar *name, const gchar *prefix,
                      gpointer *prefix_data, const gchar *prev,
                      gpointer *prev_data, gpointer user_data)
{
  return FALSE;
}

static gboolean
vp_append_sdata_value(const gchar *name, const gchar *prefix,
                      TypeHint type, const gchar *value,
                      gpointer *prefix_data, gpointer user_data)
{
  gchar *id = NULL;
  avro_value_t sd_element;
  avro_value_t param_value;
  _NVFunctionUserData* priv_data = (_NVFunctionUserData*) user_data;
  AvroDriver* self = priv_data->driver;

  if (prefix && g_str_has_prefix(prefix, "_SDATA"))
    {
      id = g_strdup(prefix + sizeof("_SDATA"));
      assert_zero(self, avro_value_add(priv_data->parent, id, &sd_element, NULL, NULL));
      assert_zero(self, avro_value_add(&sd_element, name, &param_value, NULL, NULL));
      assert_zero(self, avro_value_set_string(&param_value, value));
    }
  g_free(id);

  return FALSE;
}

static gint
avro_mod_dd_set_sdata(AvroDriver *self, avro_value_t *parent, LogMessage *logmsg)
{
  int error;
  avro_value_t field;
  avro_value_t branch;
  _NVFunctionUserData priv_data;

  error = assert_zero(self, avro_value_get_by_name(parent, "STRUCTURED_DATA", &field, NULL));

  if (logmsg == NULL || logmsg->num_sdata == 0)
    {
      error = assert_zero(self, avro_value_set_branch(&field, NULL_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_null(&branch));
    }
  else
    {
      assert_zero(self, avro_value_set_branch(&field, VALUE_BRANCH, &branch));
      priv_data.driver = self;
      priv_data.parent = &branch;
      priv_data.logmsg = logmsg;

      value_pairs_walk(self->vp,
                       vp_append_sdata_start,
                       vp_append_sdata_value,
                       vp_append_sdata_end,
                       logmsg,
                       self->seq_num,
                       &self->template_options,
                       &priv_data);

    }

  return error;
}

static gint
avro_mod_dd_set_message(AvroDriver* self, avro_value_t *parent, LogMessage *logmsg)
{
  int error;
  avro_value_t field;
  avro_value_t branch;
  const gchar *message;
  gssize message_size;

  error = assert_zero(self, avro_value_get_by_name(parent, "MSG", &field, NULL));

  if (logmsg == NULL ||
      !(message = log_msg_get_value(logmsg, log_msg_get_value_handle("MESSAGE"), &message_size)))
    {
      error = assert_zero(self, avro_value_set_branch(&field, NULL_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_null(&branch));
    }
  else
    {
      error = assert_zero(self, avro_value_set_branch(&field, VALUE_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_string_len(&branch, message, message_size));
    }

  return error;
}

static gint
avro_mod_dd_fill_msg(AvroDriver *self, LogMessage *logmsg, avro_value_t *avro_data)
{
  int error;

  error =  avro_mod_dd_set_priority(self, avro_data, logmsg);
  error |= avro_mod_dd_set_stamp(self, avro_data, logmsg);
  error |= avro_mod_dd_set_hostname(self, avro_data, logmsg);
  error |= avro_mod_dd_set_app_name(self, avro_data, logmsg);
  error |= avro_mod_dd_set_procid(self, avro_data, logmsg);
  error |= avro_mod_dd_set_msgid(self, avro_data, logmsg);
  error |= avro_mod_dd_set_sdata(self, avro_data, logmsg);
  error |= avro_mod_dd_set_message(self, avro_data, logmsg);

  return error;
}

static gchar *
avro_mod_dd_format_stats_instance(LogThrDestDriver *s)
{
  AvroDriver *self = (AvroDriver*) s;
  static gchar persist_name[1024];

  g_snprintf(persist_name, sizeof(persist_name),
             "avro,%s", self->file_name->template);

  return persist_name;
}

static gchar *
avro_mod_dd_format_persist_name(LogThrDestDriver *s)
{
  AvroDriver *self = (AvroDriver*) s;
  static gchar persist_name[1024];

  g_snprintf(persist_name, sizeof(persist_name),
             "avro(%s)", self->file_name->template);

  return persist_name;
}


static gboolean
avro_mod_dd_worker_insert(LogThrDestDriver *s)
{
  gboolean success;
  LogMessage *msg;
  avro_value_t avro_logmsg;
  int error;
  gboolean is_same_file;
  AvroDriver *self = (AvroDriver*) s;

  LogPathOptions path_options = LOG_PATH_OPTIONS_INIT;
  success = log_queue_pop_head(self->super.queue, &msg, &path_options, FALSE, FALSE);
  if (!success)
    return TRUE;

  msg_set_context(msg);

  log_template_format(self->file_name, msg, &self->template_options, LTZ_SEND,
                      self->seq_num, NULL, self->current_file_name);

  is_same_file = g_string_equal(self->active_file_name, self->current_file_name);

  if (!is_same_file)
    {
      if (self->active_file_name->len > 0)
        error = avro_mod_dd_datafile_close(self);
      g_string_assign(self->active_file_name, self->current_file_name->str);
      error = avro_mod_dd_datafile_open(self, self->active_file_name->str);
    }

  error = avro_mod_dd_init_msg(self, &avro_logmsg);
  error |= avro_mod_dd_fill_msg(self, msg, &avro_logmsg);
  error |= avro_mod_dd_datafile_append_msg(self, &avro_logmsg);
  avro_mod_dd_free_msg(&avro_logmsg);

  success = (error == 0) ? TRUE : FALSE;

  if (success)
    {
      stats_counter_inc(self->super.stored_messages);
      step_sequence_number(&self->seq_num);
      log_msg_ack(msg, &path_options);
      log_msg_unref(msg);
    }
  else
    {
      log_queue_push_head(self->super.queue, msg, &path_options);
    }

  return success;
}

/*
 * Main thread
 */

static gboolean
avro_mod_dd_init(LogPipe *s)
{
  AvroDriver *self = (AvroDriver *)s;
  GlobalConfig *cfg = log_pipe_get_config(s);
  ValuePairsTransformSet *vpts;

  if (!log_threaded_dest_driver_init_method(s))
    return FALSE;

  log_template_options_init(&self->template_options, cfg);

  /* Always replace a leading dot with an underscore. */
  vpts = value_pairs_transform_set_new(".*");
  value_pairs_transform_set_add_func(vpts, value_pairs_new_transform_replace_prefix(".", "_"));
  value_pairs_add_transforms(self->vp, vpts);

  self->current_file_name = g_string_sized_new(128);
  self->active_file_name = g_string_sized_new(128);

  if (self->timestamp == NULL)
    {
      self->timestamp = log_template_new(cfg, NULL);
      log_template_compile(self->timestamp, "$STAMP", NULL);
    }
  self->current_timestamp = g_string_sized_new(128);

  log_threaded_dest_driver_start(&self->super);
  return TRUE;
}

static gboolean
avro_mod_dd_deinit(LogPipe *s)
{
  AvroDriver *self = (AvroDriver *)s;
  int error;

  if (self->active_file_name->len > 0)
    error = avro_mod_dd_datafile_close(self);

  g_string_free(self->active_file_name, TRUE);
  g_string_free(self->current_file_name, TRUE);
  g_string_free(self->current_timestamp, TRUE);

  return log_threaded_dest_driver_deinit_method(s);
}

/*
 * Plugin glue.
 */

static void
avro_mod_dd_free(LogPipe *d)
{
  AvroDriver *self = (AvroDriver*) d;

  log_template_unref(self->timestamp);
  if (self->vp)
    value_pairs_free(self->vp);

  log_threaded_dest_driver_free(d);
}

LogDriver *
avro_mod_dd_new(GlobalConfig *cfg)
{
  AvroDriver *self = g_new0(AvroDriver, 1);

  log_threaded_dest_driver_init_instance(&self->super);

  self->super.super.super.super.init = avro_mod_dd_init;
  self->super.super.super.super.free_fn = avro_mod_dd_free;
  self->super.super.super.super.deinit = avro_mod_dd_deinit;

  self->super.worker.insert = avro_mod_dd_worker_insert;
  self->super.format.stats_instance = avro_mod_dd_format_stats_instance;
  self->super.format.persist_name = avro_mod_dd_format_persist_name;
  self->super.stats_source = SCS_AVRO;

  init_sequence_number(&self->seq_num);
  log_template_options_defaults(&self->template_options);
  avro_mod_dd_set_value_pairs(&self->super.super.super,
                              value_pairs_new_default(cfg));

  return (LogDriver *)self;
}

extern CfgParser avro_mod_parser;

static Plugin avro_mod_plugin =
{
  .type = LL_CONTEXT_DESTINATION,
  .name = "avro",
  .parser = &avro_mod_parser,
};

gboolean
avro_module_init(GlobalConfig *cfg, CfgArgs *args)
{
  plugin_register(cfg, &avro_mod_plugin, 1);
  return TRUE;
}

const ModuleInfo module_info =
{
  .canonical_name = "avro",
  .version = VERSION,
  .description = "The avro module provides AVRO destination and support for syslog-ng.",
  .core_revision = SOURCE_REVISION,
  .plugins = &avro_mod_plugin,
  .plugins_len = 1,
};
