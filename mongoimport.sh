#!/bin/bash

mongoimport \
  --db airbnb_madrid \
  --collection listings \
  --type csv \
  --file data/listings.csv \
  --headerline