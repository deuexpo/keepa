# keepa

Fetch a product data by ASIN:

``` python
import keepa

ASIN = 'B0009F3PQ2'
KEEPA_API_KEY = '***********'

api = keepa.KeepaAPI(KEEPA_API_KEY)
print('Tokens Left:', api.tokens_left())

product = api.products([ASIN])['products'][0]

print(product['asin'])
print(product['title'])
print('Tokens Left:', api.tokens_left())
```
