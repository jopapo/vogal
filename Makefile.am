# Copyright (C) 2005-2006 MySQL AB
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

#called from the top level Makefile

MYSQLDATAdir =          $(localstatedir)
MYSQLSHAREdir =         $(pkgdatadir)
MYSQLBASEdir=           $(prefix)
MYSQLLIBdir=            $(pkglibdir)
INCLUDES =              -I$(top_builddir)/include \
			-I$(top_srcdir)/include \
			-I$(top_srcdir)/regex \
			-I$(top_srcdir)/sql \
                        -I$(srcdir)
LDADD =

DEFS = @DEFS@

noinst_HEADERS = ha_vogal.h

EXTRA_LTLIBRARIES =	ha_vogal.la
pkglib_LTLIBRARIES =	@plugin_vogal_shared_target@
ha_vogal_la_LDFLAGS =	-module -rpath $(MYSQLLIBdir)
ha_vogal_la_CXXFLAGS =	$(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
ha_vogal_la_SOURCES =	ha_vogal.cc vogal_handler.cc vogal_storage.cc vogal_cache.cc list.cc \
                      vogal_definition.cc vogal_manipulation.cc vogal_utils.cc BlockCursorType.cc \
                      CursorType.cc ColumnCursorType.cc DataCursorType.cc ObjectCursorType.cc \
                      RidCursorType.cc SearchInfoType.cc NodeType.cc FilterCursorType.cc

EXTRA_LIBRARIES =	libvogal.a
noinst_LIBRARIES =	@plugin_vogal_static_target@
libvogal_a_CXXFLAGS =	$(AM_CFLAGS)
libvogal_a_SOURCES=	$(ha_vogal_la_SOURCES)

EXTRA_DIST =		CMakeLists.txt plug.in
# Don't update the files from bitkeeper
%::SCCS/s.%
