#!/usr/bin/env bash
envsubst < /alloc/var/normalizer/config/config_env.json > /alloc/var/normalizer/config/config.json
/alloc/var/normalizer/bin/normalizer-start.sh /alloc/var/normalizer/config/config.json