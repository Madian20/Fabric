CREATE TABLE [dbo].[dim_orders] (
    [order_id]                      VARCHAR (MAX) NULL,
    [customer_id]                   VARCHAR (MAX) NULL,
    [order_status]                  VARCHAR (MAX) NULL,
    [order_purchase_timestamp]      DATETIME2 (6) NULL,
    [order_approved_at]             DATETIME2 (6) NULL,
    [order_delivered_carrier_date]  DATETIME2 (6) NULL,
    [order_delivered_customer_date] DATETIME2 (6) NULL,
    [order_estimated_delivery_date] DATETIME2 (6) NULL
);


GO