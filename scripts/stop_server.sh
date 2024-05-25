#!/bin/bash

ps | grep NamiServer | cut -w -f 1 | xargs kill
