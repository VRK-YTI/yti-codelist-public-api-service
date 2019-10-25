# Pull base image
FROM yti-docker-java-base:alpine

# Deploy project
ADD target/yti-codelist-public-api-service.jar ${deploy_dir}/yti-codelist-public-api-service.jar

# Expose port
EXPOSE 9601

# Set default command on run
ENTRYPOINT ["/bootstrap.sh", "yti-codelist-public-api-service.jar"]
