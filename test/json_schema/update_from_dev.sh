# Items
wget -O ./item-spec/json-schema/basics.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/item-spec/json-schema/basics.json
wget -O ./item-spec/json-schema/datetime.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/item-spec/json-schema/datetime.json
wget -O ./item-spec/json-schema/instrument.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/item-spec/json-schema/instrument.json
wget -O ./item-spec/json-schema/item.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/item-spec/json-schema/item.json
wget -O ./item-spec/json-schema/licensing.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/item-spec/json-schema/licensing.json
wget -O ./item-spec/json-schema/provider.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/item-spec/json-schema/provider.json

# Catalog
wget -O ./catalog-spec/json-schema/catalog.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/catalog-spec/json-schema/catalog.json
wget -O ./catalog-spec/json-schema/catalog-core.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/catalog-spec/json-schema/catalog-core.json

# Collection
wget -O ./collection-spec/json-schema/collection.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/collection-spec/json-schema/collection.json

# Change to use "$ref": "item.json#/definitions/core"
#wget -O item.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/item-spec/json-schema/item.json
#wget -O catalog.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/catalog-spec/json-schema/catalog.json
#wget -O collection.json https://raw.githubusercontent.com/radiantearth/stac-spec/dev/collection-spec/json-schema/collection.json
