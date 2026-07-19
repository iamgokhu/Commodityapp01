import json
with open('output/commodity_data_consolidated.json') as f:
    data = json.load(f)
print('Segments:', len(data))
for k, v in list(data.items())[:3]:
    print(f'  {k}: {v["total_entities"]} entities, sources: {v["sources_used"]}')