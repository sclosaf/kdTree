UPMEM_DIRS   = /usr/include/dpu

CC_HOST      = gcc
CFLAGS_HOST  = -O3 -Wall -Iinclude $(addprefix -I, $(UPMEM_DIRS))

CC_DPU       = dpu-upmem-dpurte-clang
CFLAGS_DPU   = -O2 -Iinclude

TARGET_HOST  = main
DPU_PROGRAM  = dpu_main

BUILD_DIR    = build
HOST_BUILD   = $(BUILD_DIR)/host
DPU_BUILD    = $(BUILD_DIR)/dpu

HOST_SRCS    = $(wildcard src/host/*.c)
DPU_SRCS     = $(wildcard src/dpu/*.c)

HOST_OBJS    = $(patsubst src/host/%.c,$(HOST_BUILD)/%.o,$(HOST_SRCS))
DPU_OBJS     = $(patsubst src/dpu/%.c,$(DPU_BUILD)/%.o,$(DPU_SRCS))

HOST_TARGET  = $(BUILD_DIR)/$(TARGET_HOST)
DPU_TARGET   = $(BUILD_DIR)/$(DPU_PROGRAM).dpu

$(HOST_BUILD) $(DPU_BUILD):
	mkdir -p $@

all: $(HOST_TARGET) $(DPU_TARGET)

$(HOST_BUILD)/%.o: src/host/%.c | $(HOST_BUILD)
	$(CC_HOST) $(CFLAGS_HOST) -c $< -o $@

$(DPU_BUILD)/%.o: src/dpu/%.c | $(DPU_BUILD)
	$(CC_DPU) $(CFLAGS_DPU) -c $< -o $@

$(HOST_TARGET): $(HOST_OBJS)
	$(CC_HOST) -o $@ $^ $(LDFLAGS_HOST)

$(DPU_TARGET): $(DPU_OBJS)
	$(CC_DPU) -o $@ $^ $(CFLAGS_DPU)

clean:
	rm -rf $(BUILD_DIR)

run: $(HOST_TARGET) $(DPU_TARGET)
	./$(HOST_TARGET)

.PHONY: all clean run
