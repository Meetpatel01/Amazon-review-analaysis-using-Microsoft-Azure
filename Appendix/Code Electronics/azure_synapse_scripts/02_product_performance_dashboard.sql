USE AmazonReviewAnalytics;
GO

-- Product Performance Dashboard View (Pre-aggregated)
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_product_performance_dashboard')
BEGIN
    DROP VIEW v_product_performance_dashboard;
END
GO

CREATE VIEW v_product_performance_dashboard AS
SELECT 
    asin_unified,
    LEFT(product_title, 200) as product_title_short,
    brand,
    main_category,
    product_price,
    review_year,
    review_month,
    
    -- Price Categories (Pre-calculated)
    CASE 
        WHEN product_price <= 50 THEN 'Budget (≤$50)'
        WHEN product_price <= 200 THEN 'Mid-Range ($51-$200)'
        WHEN product_price <= 500 THEN 'Premium ($201-$500)'
        ELSE 'Luxury (>$500)'
    END as price_category,
    
    -- Core Performance Metrics (Pre-calculated)
    COUNT(*) as total_reviews,
    AVG(rating) as avg_rating,
    AVG(ensemble_score) as avg_sentiment_score,
    AVG(sentiment_confidence) as avg_confidence,
    
    -- Sentiment Analysis (Pre-calculated)
    SUM(CASE WHEN final_sentiment = 'positive' THEN 1 ELSE 0 END) as positive_reviews,
    SUM(CASE WHEN final_sentiment = 'negative' THEN 1 ELSE 0 END) as negative_reviews,
    SUM(CASE WHEN final_sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral_reviews,
    ROUND(SUM(CASE WHEN final_sentiment = 'positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as positive_percentage,
    ROUND(SUM(CASE WHEN final_sentiment = 'negative' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as negative_percentage,
    
    -- Quality Metrics (Pre-calculated)
    AVG(review_quality_score) as avg_quality_score,
    AVG(helpfulness_ratio) as avg_helpfulness,
    SUM(CASE WHEN is_verified = 1 THEN 1 ELSE 0 END) as verified_reviews,
    ROUND(SUM(CASE WHEN is_verified = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as verified_purchase_rate,
    
    -- Advanced Metrics (Pre-calculated)
    AVG(sentiment_volatility) as avg_volatility,
    AVG(text_emotionality) as avg_emotionality,
    AVG(word_count) as avg_word_count,
    AVG(reading_ease) as avg_readability,
    
    -- Performance Categories (Pre-calculated)
    CASE 
        WHEN AVG(rating) >= 4.5 THEN 'Excellent (≥4.5)'
        WHEN AVG(rating) >= 4.0 THEN 'Good (4.0-4.4)'
        WHEN AVG(rating) >= 3.0 THEN 'Average (3.0-3.9)'
        ELSE 'Poor (<3.0)'
    END as performance_category,
    
    CASE 
        WHEN COUNT(*) >= 1000 THEN 'High Volume (≥1000)'
        WHEN COUNT(*) >= 100 THEN 'Medium Volume (100-999)'
        WHEN COUNT(*) >= 10 THEN 'Low Volume (10-99)'
        ELSE 'Very Low Volume (<10)'
    END as volume_category,
    
    -- Business Intelligence (Pre-calculated)
    ROUND(SUM(CASE WHEN rating >= 4.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as high_rating_percentage,
    CASE 
        WHEN AVG(product_price) > 0 THEN AVG(rating) / AVG(product_price)
        ELSE NULL
    END as price_performance_ratio
    
FROM v_gold_reviews_complete
WHERE asin_unified IS NOT NULL 
    AND asin_unified != ''
    AND review_year IS NOT NULL
GROUP BY asin_unified, LEFT(product_title, 200), brand, main_category, product_price,
         review_year, review_month,
         CASE 
             WHEN product_price <= 50 THEN 'Budget (≤$50)'
             WHEN product_price <= 200 THEN 'Mid-Range ($51-$200)'
             WHEN product_price <= 500 THEN 'Premium ($201-$500)'
             ELSE 'Luxury (>$500)'
         END;
GO

-- Test the product performance dashboard
SELECT TOP 10 
    product_title_short,
    brand,
    price_category,
    total_reviews,
    avg_rating,
    positive_percentage,
    performance_category,
    volume_category
FROM v_product_performance_dashboard
ORDER BY total_reviews DESC;