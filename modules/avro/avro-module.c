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

#include <signal.h>
#include <assert.h>

typedef struct
{
  LogDestDriver super;
  avro_schema_t schema;
  avro_value_iface_t *writer_iface;
  avro_file_writer_t data_file_writer;

  gchar *file_name;

  StatsCounterItem *dropped_messages;
  StatsCounterItem *stored_messages;

  /* Thread related stuff; shared */
  GThread *writer_thread;
  GMutex *suspend_mutex;
  GCond *writer_thread_wakeup_cond;

  gboolean writer_thread_terminate;
  gboolean writer_thread_suspended;
  GTimeVal writer_thread_suspend_target;

  LogQueue *queue;

} AvroDriver;

/*
 * Called by YACC
 */

void
avro_mod_dd_set_destination_file(LogDriver *d, const gchar *filename)
{
  AvroDriver *self = (AvroDriver *)d;

  msg_debug("Set Avro datafile",
            evt_tag_str("filename", filename),
            NULL);

  g_free(self->file_name);
  self->file_name = g_strdup(filename);
}

static inline gint
assert_zero(AvroDriver *self, gint result)
{
  if (result != 0)
    msg_error("Avro error", evt_tag_str("error", avro_strerror()),
              evt_tag_str("driver", self->super.super.id), NULL);

  return result;
}

static inline void*
assert_not_null(AvroDriver *self, void *result)
{
  if (result == NULL)
    msg_error("Avro error", evt_tag_str("error", avro_strerror()),
              evt_tag_str("driver", self->super.super.id), NULL);

  return result;
}

static void
avro_mod_dd_new_suspend(AvroDriver *self)
{
  self->writer_thread_suspended = TRUE;
  g_get_current_time(&self->writer_thread_suspend_target);
  g_time_val_add(&self->writer_thread_suspend_target,
                 self->time_reopen * 1000000);
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
    return -1;

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

  return error;
}

static gint
avro_mod_dd_init_msg(AvroDriver *self, avro_value_t *logmsg)
{
  return assert_zero(self, avro_generic_value_new(self->writer_iface, logmsg));
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
avro_mod_dd_set_timestamp(AvroDriver* self, avro_value_t *parent, LogMessage *logmsg)
{
  int error;
  GString *ts;
  avro_value_t field;
  avro_value_t branch;

  error = assert_zero(self, avro_value_get_by_name(parent, "TIMESTAMP", &field, NULL));

  if (&logmsg->timestamps[LM_TS_STAMP] != NULL)
    {
      ts = g_string_new("");
      log_stamp_format(&logmsg->timestamps[LM_TS_STAMP], ts, TS_FMT_ISO, -1, 0);
      error = assert_zero(self, avro_value_set_branch(&field, VALUE_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_string(&branch, ts->str));
      g_string_free(ts, TRUE);
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
  int error;
  const gchar *host;
  gssize size;
  avro_value_t field;
  avro_value_t branch;

  error = avro_value_get_by_name(parent, "HOSTNAME", &field, NULL);

  if (logmsg != NULL)
    {
      host = log_msg_get_value(logmsg, log_msg_get_value_handle("HOST"), &size);
      error = assert_zero(self, avro_value_set_branch(&field, VALUE_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_string(&branch, host));
    }
  else
    {
      error = assert_zero(self, avro_value_set_branch(&field, NULL_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_null(&branch));
    }

  return error;
}

static gint
avro_mod_dd_set_app_name(AvroDriver* self, avro_value_t *parent, LogMessage *logmsg)
{
  int error;
  avro_value_t field;
  avro_value_t branch;
  const gchar *app_name;
  gssize app_name_size;

  error = assert_zero(self, avro_value_get_by_name(parent, "APP_NAME", &field, NULL));

  if (logmsg == NULL ||
      !(app_name = log_msg_get_value(logmsg, log_msg_get_value_handle("PROGRAM"), &app_name_size)))
    {
      error = assert_zero(self, avro_value_set_branch(&field, NULL_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_null(&branch));
    }
  else
    {
      error = assert_zero(self, avro_value_set_branch(&field, VALUE_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_string_len(&branch, app_name, app_name_size));
    }

  return error;
}

static gint
avro_mod_dd_set_procid(AvroDriver* self, avro_value_t *parent, LogMessage *logmsg)
{
  int error;
  avro_value_t field;
  avro_value_t branch;
  const gchar * pid;
  gssize size;

  error = assert_zero(self, avro_value_get_by_name(parent, "PROCID", &field, NULL));

  if (logmsg == NULL ||
      ((pid = log_msg_get_value(logmsg, log_msg_get_value_handle("PID"), &size)) && !(size > 0)))
    {
      error = assert_zero(self, avro_value_set_branch(&field, NULL_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_null(&branch));
    }
  else
    {
      error = assert_zero(self, avro_value_set_branch(&field, VALUE_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_string_len(&branch, pid, size));
    }

  return error;
}

static gint
avro_mod_dd_set_msgid(AvroDriver* self, avro_value_t *parent, LogMessage *logmsg)
{
  int error;
  avro_value_t field;
  avro_value_t branch;
  const gchar* msgid = NULL;
  gssize size;

  error = assert_zero(self, avro_value_get_by_name(parent, "MSGID", &field, NULL));

  if (logmsg == NULL ||
      ((msgid = log_msg_get_value(logmsg, log_msg_get_value_handle("MSGID"), &size)) && !(size > 0)))
    {
      error = assert_zero(self, avro_value_set_branch(&field, NULL_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_null(&branch));
    }
  else
    {
      error = assert_zero(self, avro_value_set_branch(&field, VALUE_BRANCH, &branch));
      error = assert_zero(self, avro_value_set_string_len(&branch, msgid, size));
    }

  return error;
}

static const gchar*
remove_sdata_prefix(const gchar* name)
{
  return name + strlen(SDATA_PREFIX);
}

static gboolean
split_sdata_parts(const gchar* sdata, gchar** id, gchar** param)
{
  gchar* dot_pos;

  const gchar* sdata_without_sdata_prefix = remove_sdata_prefix(sdata);
  dot_pos = strchr(sdata_without_sdata_prefix, '.');

  if (dot_pos == NULL)
    {
      msg_error("Avro destiantion driver error",
                evt_tag_str("error", "Unable to find a separator dot in sdata"),
                NULL);
      id = param = NULL;
      return FALSE;
    }
  *id = g_strndup(sdata_without_sdata_prefix, dot_pos - sdata_without_sdata_prefix);
  *param = g_strdup(dot_pos + 1);

  return TRUE;
}

typedef struct
{
  AvroDriver *driver;
  avro_value_t *parent;
  LogMessage *logmsg;
} _NVFunctionUserData;

static gboolean
append_sdata(NVHandle handle, const gchar *name, const gchar *value, gssize value_len, gpointer user_data)
{
  gchar* id = NULL;
  gchar* param = NULL;
  avro_value_t sd_element;
  avro_value_t param_value;
  _NVFunctionUserData* priv_data = (_NVFunctionUserData*) user_data;
  AvroDriver* self = priv_data->driver;

  if (log_msg_is_handle_sdata(handle))
    {
      split_sdata_parts(name, &id, &param);
      assert_zero(self, avro_value_add(priv_data->parent, id, &sd_element, NULL, NULL));
      assert_zero(self, avro_value_add(&sd_element, param, &param_value, NULL, NULL));
      assert_zero(self, avro_value_set_string(&param_value, value));
    }

  g_free(id);
  g_free(param);

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
      log_msg_nv_table_foreach(logmsg->payload, append_sdata, &priv_data);
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
  error |= avro_mod_dd_set_timestamp(self, avro_data, logmsg);
  error |= avro_mod_dd_set_hostname(self, avro_data, logmsg);
  error |= avro_mod_dd_set_app_name(self, avro_data, logmsg);
  error |= avro_mod_dd_set_procid(self, avro_data, logmsg);
  error |= avro_mod_dd_set_msgid(self, avro_data, logmsg);
  error |= avro_mod_dd_set_sdata(self, avro_data, logmsg);
  error |= avro_mod_dd_set_message(self, avro_data, logmsg);

  return error;
}

static gchar *
avro_dd_format_stats_instance(AvroDriver *self)
{
  static gchar persist_name[1024];

  g_snprintf(persist_name, sizeof(persist_name),
             "avro,%s", self->file_name);

  return persist_name;
}

static gboolean
avro_mod_dd_worker_insert(AvroDriver *self)
{
  gboolean success;
  LogMessage *msg;
  avro_value_t avro_logmsg;
  int error;
  LogPathOptions path_options = LOG_PATH_OPTIONS_INIT;
  success = log_queue_pop_head(self->queue, &msg, &path_options, FALSE, FALSE);
  if (!success)
    return TRUE;

  msg_set_context(msg);

  error = assert_zero(self, avro_generic_value_new(self->writer_iface, &avro_logmsg));

  error |= avro_mod_dd_init_msg(self, &avro_logmsg);
  error |= avro_mod_dd_fill_msg(self, msg, &avro_logmsg);
  error |= avro_mod_dd_datafile_append_msg(self, &avro_logmsg);

  avro_value_iface_decref(self->writer_iface);
  avro_value_decref(&avro_logmsg);

  success = (error == 0) ? TRUE : FALSE;

  if (success)
    {
      stats_counter_inc(self->stored_messages);
      log_msg_ack(msg, &path_options);
      log_msg_unref(msg);
    }
  else
    {
      log_queue_push_head(self->queue, msg, &path_options);
    }

  return success;
}

static void
avro_mod_dd_new_message_became_available_in_the_queue(gpointer user_data)
{
  AvroDriver *self = (AvroDriver *) user_data;

  g_mutex_lock(self->suspend_mutex);
  g_cond_signal(self->writer_thread_wakeup_cond);
  g_mutex_unlock(self->suspend_mutex);
}

static gpointer
avro_mod_dd_worker_thread(gpointer arg)
{
  AvroDriver *self = (AvroDriver *)arg;

  msg_debug("Worker thread started",
            evt_tag_str("driver", self->super.super.id),
            NULL);

  while (!self->writer_thread_terminate)
    {
      g_mutex_lock(self->suspend_mutex);
      if (self->writer_thread_suspended)
        {
          g_cond_timed_wait(self->writer_thread_wakeup_cond,
                            self->suspend_mutex,
                            &self->writer_thread_suspend_target);
          self->writer_thread_suspended = FALSE;
          g_mutex_unlock(self->suspend_mutex);
        }
      else if (!log_queue_check_items(self->queue, NULL, avro_mod_dd_new_message_became_available_in_the_queue, self, NULL))
        {
          g_cond_wait(self->writer_thread_wakeup_cond, self->suspend_mutex);
          g_mutex_unlock(self->suspend_mutex);
        }
      else
        {
          g_mutex_unlock(self->suspend_mutex);
        }

      if (self->writer_thread_terminate)
        break;

      if (!avro_mod_dd_worker_insert(self))
        {
          avro_mod_dd_new_suspend(self);
        }
    }

  msg_debug("Worker thread finished",
            evt_tag_str("driver", self->super.super.id),
            NULL);

  return NULL;
}

/*
 * Main thread
 */

static void
avro_mod_dd_start_thread(AvroDriver *self)
{
  self->writer_thread = create_worker_thread(avro_mod_dd_worker_thread, self, TRUE, NULL);
}

static void
avro_mod_dd_stop_thread(AvroDriver *self)
{
  self->writer_thread_terminate = TRUE;
  g_mutex_lock(self->suspend_mutex);
  g_cond_signal(self->writer_thread_wakeup_cond);
  g_mutex_unlock(self->suspend_mutex);
  g_thread_join(self->writer_thread);
}

static gboolean
avro_mod_dd_init(LogPipe *s)
{
  AvroDriver *self = (AvroDriver *)s;
  GlobalConfig *cfg = log_pipe_get_config(s);
  gboolean error;

  error = avro_mod_dd_datafile_open(self, self->file_name);

  if (!error)
    {
      msg_error("Unable to open Avro datafile",
                evt_tag_str("driver", self->super.super.id),
                evt_tag_str("filename", self->file_name),
                NULL);
      return FALSE;
    }

  msg_verbose("Avro datafile has been opened",
              evt_tag_str("driver", self->super.super.id),
              evt_tag_str("filename", self->file_name),
              NULL);

  self->queue = log_dest_driver_acquire_queue(&self->super, avro_dd_format_stats_instance(self));

  stats_lock();
  stats_register_counter(0, SCS_AVRO | SCS_DESTINATION, self->super.super.id,
                         avro_dd_format_stats_instance(self),
                         SC_TYPE_STORED, &self->stored_messages);
  stats_register_counter(0, SCS_AVRO | SCS_DESTINATION, self->super.super.id,
                         avro_dd_format_stats_instance(self),
                         SC_TYPE_DROPPED, &self->dropped_messages);
  stats_unlock();

  avro_mod_dd_start_thread(self);

  return TRUE;
}

static gboolean
avro_mod_dd_deinit(LogPipe *s)
{
  AvroDriver *self = (AvroDriver *)s;
  int error;

  error = avro_mod_dd_datafile_close(self);
  if (error)
    {
      msg_error("Unable to close Avro datafile",
                evt_tag_str("driver", self->super.super.id),
                evt_tag_str("filename", self->file_name),
                NULL);
    }

  msg_debug("Avro datafile closed",
            evt_tag_str("driver", self->super.super.id),
            evt_tag_str("filename", self->file_name),
            NULL);

  avro_mod_dd_stop_thread(self);
  log_queue_reset_parallel_push(self->queue);

  stats_lock();
  stats_unregister_counter(SCS_AVRO | SCS_DESTINATION, self->super.super.id,
                           avro_dd_format_stats_instance(self),
                           SC_TYPE_STORED, &self->stored_messages);
  stats_unregister_counter(SCS_AVRO | SCS_DESTINATION, self->super.super.id,
                           avro_dd_format_stats_instance(self),
                           SC_TYPE_DROPPED, &self->dropped_messages);
  stats_unlock();

  return TRUE;
}

static void
avro_mod_dd_free(LogPipe *d)
{
  AvroDriver *self = (AvroDriver *)d;

  g_mutex_free(self->suspend_mutex);
  g_cond_free(self->writer_thread_wakeup_cond);

  if (self->queue)
    log_queue_unref(self->queue);

  log_dest_driver_free(d);
}

static void
avro_mod_dd_queue(LogPipe *s, LogMessage *msg,
                  const LogPathOptions *path_options, gpointer user_data)
{
  AvroDriver *self = (AvroDriver *)s;
  LogPathOptions local_options;

  if (!path_options->flow_control_requested)
    path_options = log_msg_break_ack(msg, path_options, &local_options);

  log_msg_add_ack(msg, path_options);
  log_queue_push_tail(self->queue, log_msg_ref(msg), path_options);

  log_dest_driver_queue_method(s, msg, path_options, user_data);
}

/*
 * Plugin glue.
 */

LogDriver *
avro_mod_dd_new(void)
{
  AvroDriver *self = g_new0(AvroDriver, 1);

  log_dest_driver_init_instance(&self->super);
  self->super.super.super.init = avro_mod_dd_init;
  self->super.super.super.deinit = avro_mod_dd_deinit;
  self->super.super.super.queue = avro_mod_dd_queue;
  self->super.super.super.free_fn = avro_mod_dd_free;
  self->writer_thread_wakeup_cond = g_cond_new();
  self->suspend_mutex = g_mutex_new();

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
