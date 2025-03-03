#!/bin/bash

set -eu
set -o pipefail

tr -d '{}' <$1 | \
	sed -E 's/([[:digit:]], ?)/\1\n/g'
