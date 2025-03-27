# Compiler and flags
CC = gcc
# Add include paths for src and its subdirectories
CFLAGS = -Wall -Wextra -g -Isrc -Isrc/database -Isrc/btree

# Directories
SRC_DIR = src
BUILD_DIR = build
BIN_DIR = bin
DATA_DIR = db_data

# Source files: Find all .c files under SRC_DIR
SRCS := $(shell find $(SRC_DIR) -name '*.c')

# Object files: Create target .o names by taking the basename of the .c file
# and placing them all directly in the BUILD_DIR.
# Example: src/database/database.c -> build/database.o
OBJS := $(patsubst %.c, $(BUILD_DIR)/%.o, $(notdir $(SRCS)))

# Executable name
TARGET = $(BIN_DIR)/db_engine

# Tell make where to find source files (current dir and all subdirs of SRC_DIR)
VPATH = $(shell find $(SRC_DIR) -type d)

# Default target
all: $(TARGET)

# Link the executable
# $^ represents all prerequisites (the .o files listed in OBJS)
# $@ represents the target (the executable file)
# | $(BIN_DIR) means $(BIN_DIR) must exist but doesn't trigger relinking if only the dir timestamp changes
$(TARGET): $(OBJS) | $(BIN_DIR)
	$(CC) $^ -o $@ $(LDFLAGS)
	@echo "Executable created: $@"

# Generic rule to compile .c files into the BUILD_DIR
# Make uses VPATH to find the source file (%.c) corresponding to the target (build/%.o)
# $< represents the found source file (e.g., src/database/database.c)
# $@ represents the target object file (e.g., build/database.o)
# | $(BUILD_DIR) means $(BUILD_DIR) must exist first
$(BUILD_DIR)/%.o: %.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@
	@echo "Compiled $< -> $@"

# Create directories if they don't exist
# Use order-only prerequisites (|) to prevent unnecessary rebuilds
$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)
	@echo "Created directory: $(BUILD_DIR)"

$(BIN_DIR):
	@mkdir -p $(BIN_DIR)
	@echo "Created directory: $(BIN_DIR)"

# Clean up build artifacts and the data directory
clean:
	@echo "Cleaning build artifacts and data directory..."
	@rm -rf $(BUILD_DIR) $(BIN_DIR) $(DATA_DIR)
	@echo "Clean complete."

# Phony targets (targets that aren't actual files)
.PHONY: all clean print_vars

# Debug: Print variables to help understand the Makefile
print_vars:
	@echo "--- Makefile Variables ---"
	@echo "SRCS=$(SRCS)"
	@echo "OBJS=$(OBJS)"
	@echo "VPATH=$(VPATH)"
	@echo "TARGET=$(TARGET)"
	@echo "CFLAGS=$(CFLAGS)"
	@echo "--------------------------"
