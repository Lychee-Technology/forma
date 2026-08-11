# 实体间关系的处理

在数据库设计和数据建模中，实体间的关系是一个重要的概念。本文档将介绍如何在系统中处理实体间的关系，包括一对一、一对多关系。Forma暂时不支持多对多关系。

## 1:N关系

在1:N （N >= 0） 关系中，一个实体实例可以关联多个另一个实体实例。例如，一个“作者”可以有多本“书籍”。在数据库中，这通常通过在“多”端的实体表中添加一个外键字段来实现，该字段引用“单”端实体的主键。

在Forma中，可以通过在JSON Schema中使用`x-relation`扩展属性来定义实体间的关系。Forma会根据这些定义自动处理数据的关联，并在查询时“多”端的实体时，自动根据关联关系加载“单”端的数据。


以下是一个示例，展示了如何定义一个“作者”实体与多个“书籍”实体之间的一对多关系：

* Author实体的JSON Schema定义 (author.json)：
```json
{
  "type": "object",
  "$defs": {
    "author_id": {
      "type": "string",
      "minLength": 5,
      "maxLength": 10
    },
     "author_name": {
      "type": "string",
      "minLength": 1,
      "maxLength": 20
    }
  },
  "properties": {
    "id": {
      "$ref": "#/$defs/author_id"
    },
    "name": {
      "$ref": "#/$defs/author_name"
    }
  }
}
```

* Book实体的JSON Schema定义 (book.json)：
```json
{
  "type": "object",
  "$defs": {
    "book_id": {
      "type": "string",
      "minLength": 5,
      "maxLength": 10
    }
  },
  "properties": {
    "book_id": {
      "$ref": "#/$defs/book_id"
    },
    "title": {
      "type": "string"
    },
    "author_id": {
      "$ref": "author.json#/$defs/author_id",
    },
    "author_name": {
      "$ref": "author.json#/$defs/author_name",
      "x-relation": {
        "key_property":  "author_id"
      }
    }
  }
}
``` 

在上述示例中，`Book`实体通过`author_id`字段与`Author`实体建立了一对多关系。`author_name`字段使用了`x-relation`扩展属性，指定了关联的键属性为`author_id`。这样，当查询`Book`实体时，Forma会自动加载对应的`Author`实体的名称。

### `x-relation`只在根级`properties`中被识别

Forma只读取schema文档自身顶层`properties`对象里的`x-relation`标记：它不会展开`allOf`等组合关键字，也不会跟随`$ref`去别的文档里找。因此写在`allOf`分支里的、或写在根级`$ref`所指向的文档里的`x-relation`，Forma根本不会发现——该属性不会被剥离，读取时也不会由父实体填充，它就是一个普通的、由调用方写入并持久化的属性。

这种情况不会导致启动失败，实体也照样可读可写；关系只是没有生效，而文档看上去却像是声明了关系，所以很容易被忽略。请把`x-relation`直接声明在根级`properties`的属性上。