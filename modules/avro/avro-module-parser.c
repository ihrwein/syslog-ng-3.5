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

#include "avro-module.h"
#include "cfg-parser.h"
#include "avro-module-parser.h"
#include "avro-module-grammar.h"

extern int avro_mod_debug;
int avro_mod_parse(CfgLexer *lexer, LogDriver **instance, gpointer arg);

static CfgLexerKeyword avro_mod_keywords[] = {
  { "stamp",                    KW_AVRO_STAMP },
  { "avro",			KW_AVRO },
  { NULL },
};

CfgParser avro_mod_parser =
{
#if ENABLE_DEBUG
  .debug_flag = &avro_mod_debug,
#endif
  .name = "avro_mod",
  .keywords = avro_mod_keywords,
  .parse = (int (*)(CfgLexer *lexer, gpointer *instance, gpointer)) avro_mod_parse,
  .cleanup = (void (*)(gpointer)) log_pipe_unref,
};

CFG_PARSER_IMPLEMENT_LEXER_BINDING(avro_mod_, LogDriver **)
