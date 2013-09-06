/*
 * Copyright (c) 2011-2012 BalaBit IT Ltd, Budapest, Hungary
 * Copyright (c) 2011-2012 Gergely Nagy <algernon@balabit.hu>
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

#ifndef AVRO_MODULE_H_INCLUDED
#define AVRO_MODULE_H_INCLUDED

#include "driver.h"
#include "avro.h"
//#include "logmsg.h"
#include <config.h>

#define AVRO_SCHEMA_FILE  PATH_DATADIR "/syslog-ng/avro-schema/logmsg.avsc"

#define NULL_BRANCH     0
#define VALUE_BRANCH    1

#define SDATA_PREFIX  ".SDATA."

#define LOG_MSG			"LOG_MSG"

#define PRIORITY		"PRIORITY"
#define _VERSION         "VERSION"
#define TIMESTAMP		"TIMESTAMP"
#define HOSTNAME		"HOSTNAME"
#define APP_NAME        "APP_NAME"
#define PROCID          "PROCID"
#define MSGID           "MSGID"
#define STRUCTURED_DATA "STRUCTURED_DATA"
#define SD_ID           "SD_ID"
#define MSG             "MSG"

LogDriver *avro_mod_dd_new(void);

void avro_mod_dd_set_destination_file(LogDriver *d, const gchar *filename);

#endif
