#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([HDF5Meta], [1.0], [zhangwei217245@gmail.com])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])
LT_INIT
# Checks for programs.
AC_PROG_CC
AC_CONFIG_FILES([Makefile lib/Makefile src/Makefile])

m4_ifdef([AM_PROG_AR], [AM_PROG_AR])
m4_ifdef([AC_PROG_LIB],[AC_PROG_LIB],[m4_warn(portability,[Missing AC_PROJ_LIB])])

# Checks for libraries.

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_OUTPUT
