ADD_LIBRARY(string_utils SHARED string_utils.c string_utils.h)
ADD_LIBRARY(query_utils SHARED query_utils.c query_utils.h)
TARGET_LINK_LIBRARIES(query_utils string_utils)

ADD_LIBRARY(timer_utils SHARED timer_utils.c timer_utils.h)
ADD_LIBRARY(thpool SHARED thpool.c thpool.h)
ADD_LIBRARY(indexds art.c art.h hashset.c hashset.h vector.c vector.h)
ADD_LIBRARY(hdf5_meta hdf5_meta.c hdf5_meta.h)