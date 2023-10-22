#!/bin/bash

#
# Copyrighted as an unpublished work 2016 D&B.
# Proprietary and Confidential.  Use, possession and disclosure subject to license agreement.
# Unauthorized use, possession or disclosure is a violation of D&B's legal rights and may result
# in suit or prosecution.
#

#
# Truncate (aka purge) all of the data from a HBase table
# TODO: rewrite this in Java. See https://stash.aws.dnb.com/projects/TRAD/repos/ingest/pull-requests/8/overview

# Set up all the global variable we require
init() {
  local ARGUMENTS="$@"

  TABLE_LIST=

  # Parse option(s)
  while [ $# -ne 0 -a "${1:0:1}" = "-" ] ; do
    if [ "${1^^}" = "-H" -o "${1^^}" = "--HELP" -o "$1" = "-?" ]; then
      helpPage
    else
      helpPage "Unexpected option '$1'"
    fi
    shift
  done

  if [ $# -eq 0 ]; then
    helpPage "Invalid argument: ${ARGUMENTS}"
  else
    TABLE_LIST=$*
  fi
}


# Display a help page and exit the script. If, and only if, an error message is passed then return an error code
# $1...: Optional error messages
helpPage() {
  local RET

  if [ $# -eq 0 ]; then
    RET=0
  else
    RET=1

    if [ $# -ne 0 ]; then
      echo $1
      shift
    fi

    echo ""
  fi

  echo "Usage:"
  echo "  `basename $0` [OPTIONS] <name-space>:<table-name>..."
  echo
  echo "Purge all the data in a HBase table"
  echo
  echo "Options are:"
  echo "   -h, -?, --help        Display this help page and exit."
  echo
  echo "Merge "
  echo
  echo "Return Codes:"
  echo "   0 - Success"
  echo "   1 - Hadoop error"
  echo "  99 - Internal error"

  exit ${RET}
}

# Read tables known to hbase in all namespaces into ${TABLES}
# Returns: 0 The all tables were listed
#          1 the namespace does not exist
function readTables() {
  local RESULT=`echo -e "list" | hbase shell -n 2>&1`
  local RET=

  echo ${RESULT} | head -n1 | grep -q "^ERROR "
  if [ $? -eq 0 ] ; then                                # Name space not found
    RET=1
  else
    RET=0
    TABLES=`echo -e "${RESULT}" | sed '1,/^$/d' | sort | uniq`
    RET=${PIPESTATUS[0]}
  fi

  return ${RET}
}


# Purge data from table
# $1... : List of table names
purge() {
  set -u

  readTables

  local COMMAND=

  while [ $# -ne 0 ]; do
    echo "* Purging ${1}"

    local PATTERN=$( echo $1 | sed "s|\*|\.\*|g" )
    for TAB in `echo -e "${TABLES}" | grep "^${PATTERN}$"` ; do
      echo "    Purging ${TAB}"

      COMMAND="${COMMAND}\n truncate '${TAB}'"
    done

    shift
  done

  if [ -z "${COMMAND}" ]; then
    echo "WARN: No tables to purge"
  else
    local COUNT=$( echo -e " ${COMMAND}" | wc -l )
    COUNT=$(( ${COUNT} - 1 ))

    echo "  Purging ${COUNT} tables"
    echo -e "${COMMAND}" | hbase shell -n 2>/dev/null
  fi
}

#           ### Script Entry Point ##

init $*
purge ${TABLE_LIST} 2>&1


exit 0