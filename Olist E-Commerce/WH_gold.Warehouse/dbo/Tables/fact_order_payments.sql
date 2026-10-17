CREATE TABLE [dbo].[fact_order_payments] (
    [order_id]             VARCHAR (MAX)   NULL,
    [payment_sequential]   INT             NULL,
    [payment_type]         VARCHAR (MAX)   NULL,
    [payment_installments] INT             NULL,
    [payment_value]        DECIMAL (12, 2) NULL
);


GO