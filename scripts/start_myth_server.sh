#!/bin/bash

red=$(tput setaf 1)
green=$(tput setaf 2)
blue=$(tput setaf 4)
magenta=$(tput setaf 5)
cyan=$(tput setaf 6)
default=$(tput sgr0)
app/build/install/app/bin/nami-server configs/myth_5/m_00.json 2>&1 | sed "s/.*/$red&$default/" &
app/build/install/app/bin/nami-server configs/myth_5/m_01.json 2>&1 | sed "s/.*/$green&$default/" &
app/build/install/app/bin/nami-server configs/myth_5/m_02.json 2>&1 | sed "s/.*/$blue&$default/" &
app/build/install/app/bin/nami-server configs/myth_5/m_03.json 2>&1 | sed "s/.*/$magenta&$default/" &
app/build/install/app/bin/nami-server configs/myth_5/m_04.json 2>&1 | sed "s/.*/$cyan&$default/" &
