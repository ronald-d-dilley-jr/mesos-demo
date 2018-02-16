cmake_minimum_required ( VERSION 3.6 )


set ( IMAGE_NAME ${PROJECT_NAME}/framework )


add_custom_target (
    framework-image ALL
    COMMAND docker build --tag ${IMAGE_NAME}:${VERSION} --build-arg HTTPS_PROXY=$ENV{DOCKER_BUILD_PROXY} --file Dockerfile .
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
