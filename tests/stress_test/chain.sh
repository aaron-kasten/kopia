set -eao pipefail
export i1=master
for i in A B C D; do
	export i
	git checkout "pprof_extensions-$i" \
		&& git pull origin \
		&& git pull origin $i1 \
		&& git push origin
	i1="pprof_extensions-$i"
done
