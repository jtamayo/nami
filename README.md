# nami
CS244b class project

## Setup
1. Install OpenJDK 21
2. Set `JAVA_HOME`
3. Configure `$PATH` to include `JAVA_HOME`. In your .proflie, `export PATH=$JAVA_HOME/bin:$PATH`
4. Run `./gradlew build` in the root `nami` project. This should install gradle and succeed.

## To run
`/gradlew run --args='../configs/juan.json'`

## Formatting code
`./gradlew spotlessApply` (or equivalently `./gradlew sA`).
