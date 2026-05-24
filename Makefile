.PHONY: help quickstart init configure auth auth-status start stop restart status logs doctor uninstall

help:
	@echo "Targets:"
	@echo "  make quickstart   # guided setup"
	@echo "  make init         # init venv/config/service"
	@echo "  make configure    # write credentials config"
	@echo "  make auth         # run interactive 2FA bootstrap"
	@echo "  make auth-status  # show saved session age"
	@echo "  make start|stop|restart|status|logs|doctor|uninstall"

quickstart:
	./icloudctl quickstart

init:
	./icloudctl init

configure:
	./icloudctl configure

auth:
	./icloudctl auth

auth-status:
	./icloudctl auth-status

start:
	./icloudctl start

stop:
	./icloudctl stop

restart:
	./icloudctl restart

status:
	./icloudctl status

logs:
	./icloudctl logs

doctor:
	./icloudctl doctor

uninstall:
	./icloudctl uninstall
