TEMPLATE = app
CONFIG += console c++11
CONFIG -= app_bundle
CONFIG -= qt

SOURCES += \
    bloomfilter.cpp \
    correctness.cc \
    kvstore.cc\
    persistence.cc \
    skiplist.cpp \
    sstable.cpp

HEADERS += \
    bloomfilter.h \
    kvstore.h\
    kvstore_api.h\
    MurmurHash3.h\
    skiplist.h \
    sstable.h \
    test.h\
    utils.h



