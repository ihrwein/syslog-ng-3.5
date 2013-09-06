if ENABLE_AVRO
module_LTLIBRARIES			+= modules/avro/libavro.la

modules_avro_libavro_la_CFLAGS	=	\
	$(LIBAVRO_C_CFLAGS)			\
	-I$(top_srcdir)/modules/avro		\
	-I$(top_builddir)/modules/avro
modules_avro_libavro_la_SOURCES	=	\
	modules/avro/avro-module-grammar.y		\
	modules/avro/avro-module.c			\
	modules/avro/avro-module.h			\
	modules/avro/avro-module-parser.c		\
	modules/avro/avro-module-parser.h
modules_avro_libavro_la_LIBADD	=	\
	$(LIBAVRO_C_LIBS) $(MODULE_DEPS_LIBS)
modules_avro_libavro_la_LDFLAGS	=	\
	$(MODULE_LDFLAGS)
modules_avro_libavro_la_DEPENDENCIES=	\
	$(MODULE_DEPS_LIBS)

modules/avro modules/avro/ mod-avro: \
	modules/avro/libavro_module.la
else
modules/avro modules/avro/ mod-avro:
endif

BUILT_SOURCES				+=	\
	modules/avro/avro-module-grammar.y		\
	modules/avro/avro-module-grammar.c		\
	modules/avro/avro-module-grammar.h
EXTRA_DIST				+=	\
	modules/avro/avro-module-grammar.ym

.PHONY: modules/avro/ mod-avro
