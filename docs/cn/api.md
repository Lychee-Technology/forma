CRUD APIs：

* 创建  
  * `POST /api/v1/{schema_name}`  
  * `Body:` 一个 JSON 数组，其中每个元素都符合 `schema_name` 对应的JSON定义。如果只创建单条记录，也需要放在数组中。

* 单条读取  
  * `GET /api/v1/{schema_name}/{row_id}`

* 搜索 (单Schema)
  * `GET /api/v1/{schema_name}?page={page}&items_per_page={items_per_page}&{<group>:<attribute_name>}={expression}...`  
    * 分页  
      * `page`: 从1开始的页码，缺省时默认为`1`  
      * `items_per_page`: 每页条数，缺省时默认为`20`，最大不超过`100`  
    * Expression  
      * `equals`: 精确匹配，示例：`status=equals:active`  
      * `starts_with`: 前缀匹配，示例：`email=starts_with:admin@`
* 搜索 (跨Schema)
  * `GET /api/v1/search?page={page}&items_per_page={items_per_page}&{attribute_name}={expression}...`  
    * 分页与Expression与上方`GET /api/v1/{schema_name}`保持一致
* 更新  
  * `PUT /api/v1/{schema_name}/{row_id}`  
  * Body  
    * JSON, 符合 `schema_name` 对应的JSON定义
* 删除  
  * `DELETE /api/v1/{schema_name}`  
  * Body:  
    * JSON, a list of row_ids