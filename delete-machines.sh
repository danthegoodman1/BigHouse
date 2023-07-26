#!/bin/bash

while IFS= read -r line
do
  fly machines remove $line --force
done