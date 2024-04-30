.PHONY: install-swaggo
install-swaggo:
ifeq ($(wildcard $(SWAGGO_BIN)),)
	$(info #Downloading swaggo latest)
	go install github.com/swaggo/swag/cmd/swag@latest
endif

start:
	go run main.go config.yaml

# генерирует свагер
.PHONY: generate
generate: install-swaggo
	swag init -g main.go