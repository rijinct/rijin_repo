declareUpdate();
const source_count = cts.estimate(cts.jsonPropertyValueQuery("source", "source_pat"))
const iter_val = Math.round(source_count / 10000) + 2
for (let i = 1; i < iter_val; i++) {
    for (const uri of cts.uris("", "sample=10000", cts.jsonPropertyValueQuery("source", "source_pat"))) {
        xdmp.documentDelete(uri);
    }
}