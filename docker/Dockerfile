# Pull base image
FROM yti-docker-java-base:corretto-11.0.22

# Deploy project
ADD target/yti-codelist-public-api-service.jar ${deploy_dir}/yti-codelist-public-api-service.jar

# Expose port
EXPOSE 9601

# Set default command on run
ENTRYPOINT ["/bootstrap.sh", "yti-codelist-public-api-service.jar"]
