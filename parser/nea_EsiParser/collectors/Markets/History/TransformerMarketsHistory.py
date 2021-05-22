from tqdm.auto import tqdm

from ...Base import Transformer

class TransformerMarketsHistory(Transformer):
    def transform(self, responses):
        active_responses = list(filter(lambda x: x.status_code != 304, responses))
        t = tqdm(active_responses, desc='Transformer', leave=False)\
            if self.verbose else active_responses
        record_items = {}
        for response in t:
            if response.json():
                region_id = int(response.url.split('/')[-2])
                record_items[region_id] = record_items.get(region_id, [])
                record_items[region_id].extend(response.json())
        
        self.logger.info(
            'Transform process complete, %s records',
            sum([len(val) for val in record_items.values()]),
        )
        self._refresh_etags(active_responses)
        return record_items
