.PHONY: start-cockroach
start-cockroach:
	@cockroach start-single-node \
		--insecure \
		--listen-addr=localhost:26257 \
		--http-addr=localhost:8080 \
		--store=./cdata \
		--background

stop-cockroach:
	@pgrep -f "cockroach start-single-node" | xargs kill -TERM