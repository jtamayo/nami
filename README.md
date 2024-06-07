# nami
CS244b class project

## Setup
1. Install OpenJDK 21
2. Set `JAVA_HOME`
3. Configure `$PATH` to include `JAVA_HOME`. In your .proflie, `export PATH=$JAVA_HOME/bin:$PATH`
4. Run `./gradlew build` in the root `nami` project. This should install gradle and succeed.

## Running an example banking app
Build the distribution:

`./gradlew installDist`

To start each of the servers:

`app/build/install/app/bin/nami-server configs/p_00.json`
`app/build/install/app/bin/nami-server configs/p_01.json`
`app/build/install/app/bin/nami-server configs/p_02.json`

To run the client workflow:

`app/build/install/app/bin/banking-app Nami configs/client.json`

## Formatting code
`./gradlew spotlessApply` (or equivalently `./gradlew sA`).

## Installing a reasonable formatter
https://marketplace.visualstudio.com/items?itemName=JoseVSeb.google-java-format-for-vs-code
