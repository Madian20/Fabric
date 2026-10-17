CREATE TABLE [dbo].[fact_order_items] (
    [product_id]          VARCHAR (MAX)   NULL,
    [order_id]            VARCHAR (MAX)   NULL,
    [order_item_id]       INT             NULL,
    [seller_id]           VARCHAR (MAX)   NULL,
    [shipping_limit_date] DATETIME2 (6)   NULL,
    [price]               DECIMAL (12, 2) NULL,
    [freight_value]       DECIMAL (12, 2) NULL
);


GO