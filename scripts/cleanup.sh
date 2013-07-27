#!/bin/bash
rm -rf /tmp/*hadoop*

killall java
killall -9 `ps -u $USER | grep "\.sh" | awk '{ print $1 }'`

pkill java

