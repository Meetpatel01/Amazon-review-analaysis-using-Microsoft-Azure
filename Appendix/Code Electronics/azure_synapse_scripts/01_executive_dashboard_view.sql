USE AmazonReviewAnalytics;
GO

-- Executive Dashboard View (Pre-aggregated)
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_executive_dashboard')
BEGIN
    DROP VIEW v_executive_dashboard;
END
GO

CREATE VIEW v_executive_dashboard AS
SELECT 
    review_year,
    review_month,
    main_category,
    brand,
    -- Core Metrics (Pre-calculated)
    COUNT(*) as total_reviews,
    COUNT(DISTINCT asin_unified) as unique_products,
    COUNT(DISTINCT brand) as unique_brands,
    AVG(rating) as overall_customer_satisfaction,
    AVG(ensemble_score) as overall_sentiment_score,
    AVG(sentiment_confidence) as avg_confidence,
    
    -- Sentiment Distribution (Pre-calculated)
    SUM(CASE WHEN final_sentiment = 'positive' THEN 1 ELSE 0 END) as positive_reviews,
    SUM(CASE WHEN final_sentiment = 'negative' THEN 1 ELSE 0 END) as negative_reviews,
    SUM(CASE WHEN final_sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral_reviews,
    ROUND(SUM(CASE WHEN final_sentiment = 'positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as positive_percentage,
    ROUND(SUM(CASE WHEN final_sentiment = 'negative' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as negative_percentage,
    
    -- Quality Metrics (Pre-calculated)
    AVG(review_quality_score) as avg_review_quality,
    AVG(helpfulness_ratio) as avg_helpfulness,
    SUM(CASE WHEN is_verified = 1 THEN 1 ELSE 0 END) as verified_reviews,
    ROUND(SUM(CASE WHEN is_verified = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as verified_purchase_rate,
    
    -- Business Metrics (Pre-calculated)
    AVG(product_price) as avg_product_price,
    SUM(CASE WHEN rating >= 4.0 THEN 1 ELSE 0 END) as high_rated_reviews,
    ROUND(SUM(CASE WHEN rating >= 4.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as high_rating_percentage,
    
    -- NPS Score (Pre-calculated)
    ROUND((SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) - SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2) as nps_score,
    
    -- Time Periods (Pre-calculated)
    CASE WHEN review_year < 2020 THEN 'Historical (1996-2019)' ELSE 'Recent (2020-2023)' END as time_period,
    CASE WHEN review_year IN (2020, 2021) THEN 'During-COVID' 
         WHEN review_year < 2020 THEN 'Pre-COVID' 
         ELSE 'Post-COVID' END as covid_period,
    
    -- Seasonal Analysis (Pre-calculated)
    season,
    is_holiday_season,
    is_weekend
FROM v_gold_reviews_complete
WHERE review_year IS NOT NULL
GROUP BY review_year, review_month, main_category, brand,
         CASE WHEN review_year < 2020 THEN 'Historical (1996-2019)' ELSE 'Recent (2020-2023)' END,
         CASE WHEN review_year IN (2020, 2021) THEN 'During-COVID' 
              WHEN review_year < 2020 THEN 'Pre-COVID' 
              ELSE 'Post-COVID' END,
         season, is_holiday_season, is_weekend;
GO

-- Test the executive dashboard
SELECT TOP 10 
    review_year,
    main_category,
    total_reviews,
    overall_customer_satisfaction,
    positive_percentage,
    nps_score,
    avg_review_quality
FROM v_executive_dashboard
ORDER BY total_reviews DESC;