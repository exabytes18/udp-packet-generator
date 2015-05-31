#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.63])
AC_INIT([udp-packet-generator], [1.0], [exabytes18@gmail.com])

# Default compiler flags
: ${CFLAGS="--pedantic -Wall -O3 -g"}

AC_CONFIG_SRCDIR([src/udp-sender.c])
AC_CONFIG_HEADERS([src/config.h])
AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE([subdir-objects])

# Checks for programs.
AC_PROG_CC
AC_PROG_CC_C99

# Checks for pthreads.
AX_PTHREAD([], [echo "pthreads not configured properly" && exit 1])
LIBS="$PTHREAD_LIBS $LIBS"
CFLAGS="$CFLAGS $PTHREAD_CFLAGS"
CC="$PTHREAD_CC"

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h sys/time.h])

# Check for library functions
AC_CHECK_FUNCS([sendmmsg recvmmsg])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
