#!/bin/bash

# rvm env --path -- ruby-2.0.0
source /usr/local/rvm/environments/ruby-2.0.0-p353

ruby /var/ha-backup/ha-backup.rb
