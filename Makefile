HOST_INCLUDE_DIRS = /usr/include /usr/include/dpu
DPU_INCLUDE_DIRS  = /usr/share/upmem/include

CC_HOST      = gcc
CFLAGS_HOST  = -O3 -Wall -fopenmp -D_GNU_SOURCE -Iinclude $(addprefix -I, $(HOST_INCLUDE_DIRS))
LDFLAGS_HOST = -fopenmp -ldpu -lm

CC_DPU       = dpu-upmem-dpurte-clang
CFLAGS_DPU   = -Oz -flto -Iinclude $(addprefix -I, $(DPU_INCLUDE_DIRS))

BUILD_DIR    = build
HOST_BUILD   = $(BUILD_DIR)/host
DPU_BUILD    = $(BUILD_DIR)/dpu

HOST_SRCS    = $(shell find src/host -type f -name "*.c")
HOST_OBJS    = $(patsubst src/host/%.c,$(HOST_BUILD)/%.o,$(HOST_SRCS))
HOST_TARGET  = $(BUILD_DIR)/main

DPU_ALL_SRCS  = $(shell find src/dpu -type f -name "*.c")

DPU_TASK_SRCS = $(shell find src/dpu/task -type f -name "main*.c")

DPU_LIB_SRCS  = $(filter-out $(DPU_TASK_SRCS) %/main.c, $(DPU_ALL_SRCS))

DPU_LIB_OBJS  = $(patsubst src/dpu/%.c,$(DPU_BUILD)/%.o,$(DPU_LIB_SRCS))
DPU_TASK_OBJS = $(patsubst src/dpu/%.c,$(DPU_BUILD)/%.o,$(DPU_TASK_SRCS))

DPU_TARGETS   = $(patsubst src/dpu/task/main%.c,$(BUILD_DIR)/%.dpu,$(DPU_TASK_SRCS))

all: $(HOST_TARGET) $(DPU_TARGETS)

$(HOST_TARGET): $(HOST_OBJS)
	@mkdir -p $(dir $@)
	$(CC_HOST) -o $@ $^ $(LDFLAGS_HOST)

$(HOST_BUILD)/%.o: src/host/%.c
	@mkdir -p $(dir $@)
	$(CC_HOST) $(CFLAGS_HOST) -c $< -o $@

$(DPU_BUILD)/%.o: src/dpu/%.c
	@mkdir -p $(dir $@)
	$(CC_DPU) $(CFLAGS_DPU) -c $< -o $@

$(BUILD_DIR)/%.dpu: $(DPU_BUILD)/task/main%.o $(DPU_LIB_OBJS)
	@mkdir -p $(dir $@)
	@echo "Linking DPU binary: $@"
	$(CC_DPU) $(CFLAGS_DPU) -o $@ $^

clean:
	rm -rf $(BUILD_DIR)

run: all
	UPMEM_PROFILE="backend=simulator,chipId=0x42,verbose=true" ./$(HOST_TARGET)

.PHONY: all clean run
