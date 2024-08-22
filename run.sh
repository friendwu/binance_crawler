trap 'kill -9 $(jobs -p)' EXIT

for i in seq 1 10;
do
	scrapy crawl klines >> scrapy.log 2>&1
done
