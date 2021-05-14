from ....Base import Transformer

class TransformerCorpAssetsNames(Transformer):
    def transform(self, responses):
        record_items = [
            {'item_id': record['item_id'], 'item_name': record['name']}
            for response in responses
            for record in response.json()
        ]
        return record_items
