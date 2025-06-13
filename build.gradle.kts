plugins {
    id("java")
}

group = "db"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    implementation("com.oracle.database.jdbc:ojdbc11:23.7.0.25.01")
    implementation("com.oracle.database.security:oraclepki:23.7.0.25.01")
}

tasks.test {
    useJUnitPlatform()
}