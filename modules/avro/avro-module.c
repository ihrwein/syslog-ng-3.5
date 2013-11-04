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
#include <fcntl.h>
#include "logqueue.h"
#include "plugin-types.h"
#include "logthrdestdrv.h"
#include "value-pairs.h"
#include "vptransform.h"
#include <sys/stat.h>
#include <signal.h>
#include <assert.h>

const char *DEFAULT_SDATA_ID = "_DEFAULT";

typedef struct
{
  LogThrDestDriver super;
  avro_schema_t schema;
  avro_value_iface_t *writer_iface;
  avro_file_writer_t data_file_writer;
  avro_value_t avro_logmsg;
  gboolean is_datafile_opened;
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
read_whole_file(char *file_name)
{
  ssize_t read_bytes;
  struct stat file_stat;
  int file_size;

  int f = open(file_name, O_RDONLY);

  if (f == -1)
    {
      return NULL;
    }

  fstat(f, &file_stat);
  file_size = file_stat.st_size;
  char *string = (char*) malloc(file_size * sizeof(char));

  read_bytes = read(f, string, file_size);
  fill_json_string_end_with_zeros(string, file_size);
  assert(read_bytes == file_size);
  close(f);
  return string;
}

static void
avro_mod_dd_file_handle_errors(AvroDriver *self, char *filename, int error, const char *open_mode)
{
  char buffer[256];

  if (error)
    {
      snprintf(buffer, sizeof(buffer), "Unable to %s Avro datafile", open_mode);
      msg_error(buffer,
                evt_tag_str("driver", self->super.super.super.id),
                evt_tag_str("filename", filename),
                NULL);
    }
  else
    {
      snprintf(buffer, sizeof(buffer), "Avro datafile has been successfully %s(e)d", open_mode);
      msg_verbose(buffer,
                  evt_tag_str("driver", self->super.super.super.id),
                  evt_tag_str("filename", filename),
                  NULL);
      assert_zero(self, avro_generic_value_new(self->writer_iface, &self->avro_logmsg));
      self->is_datafile_opened = TRUE;
    }
}

static gint
avro_mod_dd_file_open(AvroDriver *self, char *filename)
{
  int error;

  error =  assert_zero(self, avro_file_writer_open(filename, &self->data_file_writer));
  avro_mod_dd_file_handle_errors(self, filename, error, "open");

  return error;
}

static gint
avro_mod_dd_file_create(AvroDriver *self, char *filename)
{
  int error;

  error = assert_zero(self, avro_file_writer_create(filename, self->schema, &self->data_file_writer));
  avro_mod_dd_file_handle_errors(self, filename, error, "create");

  return error;
}

static gint
avro_mod_dd_file_create_or_open(AvroDriver *self, char *filename)
{
  int error = 0;
  gboolean is_file_exist;
  gboolean is_file_writable;

  is_file_exist = access(filename, F_OK) == 0;

  if (is_file_exist)
    {
      is_file_writable = access(filename, W_OK) == 0;

      if (is_file_writable)
        {
          error |= avro_mod_dd_file_open(self, filename);
        }
      else
        {
          msg_error("Unable to open Avro datafile",
                    evt_tag_str("driver", self->super.super.super.id),
                    evt_tag_str("filename", filename),
                    evt_tag_str("details", "insufficient permissions"),
                    NULL);
          error = -1;
        }
    }
  else
    {
      error |= avro_mod_dd_file_create(self, filename);
    }

  return error;
}

static gint
avro_mod_dd_datafile_open(AvroDriver *self, char *filename)
{
  int error;
  avro_schema_error_t schema_error;
  char *schema_string = read_whole_file(AVRO_SCHEMA_FILE);

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

  error |= avro_mod_dd_file_create_or_open(self, filename);

  free(schema_string);
  return error;
}

static gint
avro_mod_dd_datafile_append_msg(AvroDriver *self, avro_value_t *avro_data)
{
  int error;

  error = assert_zero(self, avro_file_writer_append_value(self->data_file_writer, avro_data));
  error |= avro_value_reset(&self->avro_logmsg);

  return error;
}

static int
avro_mod_dd_datafile_close(AvroDriver *self)
{
  int error;

  avro_value_decref(&self->avro_logmsg);
  error = assert_zero(self, avro_file_writer_close(self->data_file_writer));
  avro_value_iface_decref(self->writer_iface);
  error |= assert_zero(self, avro_schema_decref(self->schema));

  if (error)
    {
      msg_error("Unable to close Avro datafile",
                evt_tag_str("driver", self->super.super.super.id),
                evt_tag_str("filename", self->current_file_name->str),
                NULL);
    }
  else
    {
      msg_debug("Avro datafile successfully closed",
                evt_tag_str("driver", self->super.super.super.id),
                evt_tag_str("filename", self->current_file_name->str),
                NULL);
      self->is_datafile_opened = FALSE;
    }

  return error;
}

static gint
avro_mod_dd_set_stamp(AvroDriver* self, avro_value_t *parent, LogMessage *logmsg)
{
  int error = 0;
  avro_value_t sd_element;
  avro_value_t param_value;

  log_template_format(self->timestamp, logmsg, &self->template_options, LTZ_SEND,
                      self->seq_num, NULL, self->current_timestamp);

  if (self->current_timestamp->len > 0)
    {
      error |= assert_zero(self, avro_value_add(parent, DEFAULT_SDATA_ID, &sd_element, NULL, NULL));
      error |= assert_zero(self, avro_value_add(&sd_element, "TIMESTAMP", &param_value, NULL, NULL));
      error |= assert_zero(self, avro_value_set_string(&param_value, self->current_timestamp->str));
    }

  return error;
}

typedef struct
{
  AvroDriver *driver;
  avro_value_t *parent;
  LogMessage *logmsg;
} _NVFunctionUserData;

static gboolean
avro_mod_vp_obj_start(const gchar *name, const gchar *prefix,
                      gpointer *prefix_data, const gchar *prev,
                      gpointer *prev_data, gpointer user_data)
{
  return FALSE;
}

static gboolean
avro_mod_vp_obj_end(const gchar *name, const gchar *prefix,
                      gpointer *prefix_data, const gchar *prev,
                      gpointer *prev_data, gpointer user_data)
{
  return FALSE;
}

static gboolean
avro_mod_vp_obj_value(const gchar *name, const gchar *prefix,
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
    }
  else
    {
      assert_zero(self, avro_value_add(priv_data->parent, DEFAULT_SDATA_ID, &sd_element, NULL, NULL));
    }

  assert_zero(self, avro_value_add(&sd_element, name, &param_value, NULL, NULL));
  assert_zero(self, avro_value_set_string(&param_value, value));

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

  if (logmsg == NULL)
    {
      error |= assert_zero(self, avro_value_set_branch(&field, NULL_BRANCH, &branch));
      error |= assert_zero(self, avro_value_set_null(&branch));
    }
  else
    {
      assert_zero(self, avro_value_set_branch(&field, VALUE_BRANCH, &branch));
      priv_data.driver = self;
      priv_data.parent = &branch;
      priv_data.logmsg = logmsg;

      value_pairs_walk(self->vp,
                       avro_mod_vp_obj_start,
                       avro_mod_vp_obj_value,
                       avro_mod_vp_obj_end,
                       logmsg,
                       self->seq_num,
                       &self->template_options,
                       &priv_data);

      error |= avro_mod_dd_set_stamp(self, &branch, logmsg);
    }

  return error;
}

static gint
avro_mod_dd_fill_msg(AvroDriver *self, LogMessage *logmsg, avro_value_t *avro_data)
{
  int error = 0;

  error |= avro_mod_dd_set_sdata(self, avro_data, logmsg);

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

int
avro_mod_dd_file_reopen_when_needed(AvroDriver *self)
{
  gboolean is_same_file;
  int error = 0;

  is_same_file = g_string_equal(self->active_file_name, self->current_file_name);

  if (!is_same_file || !self->is_datafile_opened)
    {
      if ((self->active_file_name->len > 0) && self->is_datafile_opened)
        {
          error |= avro_mod_dd_datafile_close(self);
        }

      g_string_assign(self->active_file_name, self->current_file_name->str);
      error |= avro_mod_dd_datafile_open(self, self->active_file_name->str);
    }

  return error;
}

static gboolean
avro_mod_dd_worker_insert(LogThrDestDriver *s)
{
  gboolean success;
  LogMessage *msg;
  int error;
  AvroDriver *self = (AvroDriver*) s;

  LogPathOptions path_options = LOG_PATH_OPTIONS_INIT;
  success = log_queue_pop_head(self->super.queue, &msg, &path_options, FALSE, FALSE);
  if (!success)
    return TRUE;

  msg_set_context(msg);

  log_template_format(self->file_name, msg, &self->template_options, LTZ_SEND,
                      self->seq_num, NULL, self->current_file_name);

  error = avro_mod_dd_file_reopen_when_needed(self);

  if (!error)
    {
      error |= avro_mod_dd_fill_msg(self, msg, &self->avro_logmsg);
      error |= avro_mod_dd_datafile_append_msg(self, &self->avro_logmsg);
    }

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

  if (self->active_file_name->len > 0)
    assert_zero(self, avro_mod_dd_datafile_close(self));

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
