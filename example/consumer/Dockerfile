# Stage 1 - Download gradle
FROM openjdk:8-jdk as gradle
ENV GRADLE_VERSION 4.10.2
ENV GRADLE_SHA b49c6da1b2cb67a0caf6c7480630b51c70a11ca2016ff2f555eaeda863143a29
RUN cd /usr/lib \
	&& curl -fl https://downloads.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -o gradle-bin.zip \
	&& echo "$GRADLE_SHA gradle-bin.zip" | sha256sum -c - \
	&& unzip "gradle-bin.zip" \
	&& ln -s "/usr/lib/gradle-${GRADLE_VERSION}/bin/gradle" /usr/bin/gradle \
	&& rm "gradle-bin.zip"

# Stage 2 assemble the ktor application
FROM openjdk:8-jdk
ENV GRADLE_VERSION 4.10.2

COPY --from=gradle /usr/lib/gradle-${GRADLE_VERSION} /usr/lib/gradle-${GRADLE_VERSION}
RUN ln -s "/usr/lib/gradle-${GRADLE_VERSION}/bin/gradle" /usr/bin/gradle

# Set Appropriate Environmental Variables
ENV GRADLE_HOME /usr/lib/gradle
ENV PATH $PATH:$GRADLE_HOME/bin

# Set the work dir
WORKDIR /home/gradle/project
COPY ./ /home/gradle/project
RUN gradle clean assemble

# Entrypoint
CMD gradle run