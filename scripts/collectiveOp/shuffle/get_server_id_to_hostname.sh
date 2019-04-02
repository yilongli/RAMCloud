#!/usr/bin/env bash

# Example usage:
# ./get_server_id_to_hostname > server_id_2_hostname.txt

ls server*log | tr . ' ' | cut -d' ' -f1,2 > server_id_2_hostname.txt