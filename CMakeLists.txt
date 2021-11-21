cmake_minimum_required(VERSION 3.14)
project(external_sort)
set(CMAKE_VERBOSE_MAKEFILE ON)

set(CMAKE_CXX_STANDARD 17)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif ()

set(CMAKE_CXX_FLAGS "-Wall -Wextra -std=c++17 -pedantic -Werror")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

include_directories(include lib)

add_executable(external_sort
        src/external_sort.cpp
        include/LightStringSortConnector.hpp )
add_executable(external_sort_numbers
        src/external_sort_numbers.cpp
        include/UnsignedLongSortConnector.hpp)
target_link_libraries(external_sort pthread)
target_link_libraries(external_sort_numbers pthread)

find_package(GTest QUIET)
if (GTest_FOUND)
    enable_testing()
    include_directories(${GTEST_INCLUDE_DIRS})


    add_executable(test_external_sort test/test_external_sort.cpp)
    target_link_libraries(test_external_sort ${DEFAULT_LIBS} ${GTEST_BOTH_LIBRARIES} pthread)
    add_test(NAME test_external_sort COMMAND ./test_external_sort)

    add_executable(test_inplace_quicksort test/test_inplace_introsort.cpp)
    target_link_libraries(test_inplace_quicksort ${DEFAULT_LIBS} ${GTEST_BOTH_LIBRARIES} pthread)
    add_test(NAME test_inplace_quicksort COMMAND ./test_inplace_quicksort)

    add_executable(test_iohandler_custom_constructor test/test_iohandler_custom_constructor.cpp)
    target_link_libraries(test_iohandler_custom_constructor ${DEFAULT_LIBS} ${GTEST_BOTH_LIBRARIES} pthread)
    add_test(NAME test_iohandler_custom_constructor COMMAND ./test_iohandler_custom_constructor)



endif ()
