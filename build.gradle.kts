plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    // https://mvnrepository.com/artifact/org.apache.spark/spark-kubernetes
    implementation("org.apache.spark:spark-kubernetes_2.12:3.4.2")
    implementation("org.apache.spark:spark-core_2.12:3.4.2")
    implementation("org.apache.spark:spark-sql_2.12:3.4.2")
    implementation("org.apache.spark:spark-catalyst_2.12:3.4.2")
    implementation("org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2")
    implementation("org.scala-lang:scala-library:2.12.10")
}



tasks.test {
    useJUnitPlatform()
}