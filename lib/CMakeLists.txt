ADD_LIBRARY(string_utils SHARED string_utils.c string_utils.h)
ADD_LIBRARY(query_utils SHARED query_utils.c query_utils.h)
TARGET_LINK_LIBRARIES(query_utils string_utils)

ADD_LIBRARY(timer_utils SHARED timer_utils.c timer_utils.h)
