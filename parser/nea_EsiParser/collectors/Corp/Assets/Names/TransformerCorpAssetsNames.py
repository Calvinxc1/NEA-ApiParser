from ....Base import Transformer

class TransformerCorpAssetsNames(Transformer):
    def transform(self, responses):
        name_lookup = {
            record['item_id']:record['name']
            for response in responses
            for record in response.json()
        }
        return name_lookup
