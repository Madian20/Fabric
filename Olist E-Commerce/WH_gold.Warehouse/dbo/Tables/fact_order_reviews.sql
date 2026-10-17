CREATE TABLE [dbo].[fact_order_reviews] (
    [order_id]                VARCHAR (MAX) NULL,
    [review_id]               VARCHAR (MAX) NULL,
    [review_score]            INT           NULL,
    [review_comment_title]    VARCHAR (MAX) NULL,
    [review_comment_message]  VARCHAR (MAX) NULL,
    [review_creation_date]    DATETIME2 (6) NULL,
    [review_answer_timestamp] DATETIME2 (6) NULL
);


GO