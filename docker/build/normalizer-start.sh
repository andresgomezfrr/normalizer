#!/usr/bin/env bash
envsubst < /opt/normalizer/config/config_env.json > /opt/normalizer/config/config.json
/opt/normalizer/bin/normalizer-start.sh /opt/normalizer/config/config.json