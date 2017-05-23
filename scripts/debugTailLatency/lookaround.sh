#!/bin/bash
LOOKAROUND=20
grep -h "sendRequest invoked, $1" -B $LOOKAROUND -A $LOOKAROUND client*.tt
echo "============================================================"
grep -h "deleted server RPC, $1" -B $LOOKAROUND -A $LOOKAROUND server*.tt
echo "============================================================"
grep -h "deleted client RPC, $1" -B $LOOKAROUND -A $LOOKAROUND client*.tt
