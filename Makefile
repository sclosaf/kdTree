HOST_INCLUDE_DIRS = /usr/include /usr/include/dpu
DPU_INCLUDE_DIRS =

CC_HOST      = gcc
CFLAGS_HOST  = -O3 -Wall -fopenmp -D_GNU_SOURCE -Iinclude $(addprefix -I, $(HOST_INCLUDE_DIRS))
LDFLAGS_HOST = -fopenmp -ldpu -lm

CC_DPU       = dpu-upmem-dpurte-clang
CFLAGS_DPU   = -O2 -Iinclude $(addprefix -I, $(DPU_INCLUDE_DIRS))

TARGET_HOST  = main
DPU_PROGRAM  = dpuMain

BUILD_DIR    = build
HOST_BUILD   = $(BUILD_DIR)/host
DPU_BUILD    = $(BUILD_DIR)/dpu

HOST_SRCS    = $(shell find src/host -name "*.c")
DPU_SRCS     = $(shell find src/dpu -name "*.c")

HOST_OBJS    = $(patsubst src/%.c,$(HOST_BUILD)/%.o,$(HOST_SRCS))
DPU_OBJS     = $(patsubst src/%.c,$(DPU_BUILD)/%.o,$(DPU_SRCS))

HOST_TARGET  = $(BUILD_DIR)/$(TARGET_HOST)
DPU_TARGET   = $(BUILD_DIR)/$(DPU_PROGRAM).dpu

$(HOST_BUILD) $(DPU_BUILD):
	mkdir -p $@

$(HOST_BUILD)/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC_HOST) $(CFLAGS_HOST) -c $< -o $@

$(DPU_BUILD)/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC_DPU) $(CFLAGS_DPU) -c $< -o $@

all: $(HOST_TARGET) $(DPU_TARGET)

$(HOST_TARGET): $(HOST_OBJS)
	$(CC_HOST) -o $@ $^ $(LDFLAGS_HOST)

$(DPU_TARGET): $(DPU_OBJS)
	$(CC_DPU) -o $@ $^

clean:
	rm -rf $(BUILD_DIR)

run: $(HOST_TARGET) $(DPU_TARGET)
	UPMEM_PROFILE="backend=simulator,chipId=0x2" ./$(HOST_TARGET)

.PHONY: all clean run
