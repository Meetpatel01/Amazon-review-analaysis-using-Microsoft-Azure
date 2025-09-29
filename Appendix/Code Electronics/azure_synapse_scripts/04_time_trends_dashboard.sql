USE AmazonReviewAnalytics;
GO

-- Time-Based Trends Dashboard View (Fixed)
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_time_trends_dashboard')
BEGIN
    DROP VIEW v_time_trends_dashboard;
END
GO

CREATE VIEW v_time_trends_dashboard AS
SELECT 
    review_year,
    review_month,
    main_category,
    brand,
    season,
    
    -- Core Metrics (Pre-calculated)
    COUNT(*) as total_reviews,
    COUNT(DISTINCT asin_unified) as unique_products,
    AVG(rating) as avg_rating,
    AVG(ensemble_score) as avg_sentiment,
    
    -- Sentiment Trends (Pre-calculated)
    SUM(CASE WHEN final_sentiment = 'positive' THEN 1 ELSE 0 END) as positive_reviews,
    SUM(CASE WHEN final_sentiment = 'negative' THEN 1 ELSE 0 END) as negative_reviews,
    ROUND(SUM(CASE WHEN final_sentiment = 'positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as positive_percentage,
    ROUND(SUM(CASE WHEN final_sentiment = 'negative' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as negative_percentage,
    
    -- Quality Trends (Pre-calculated)
    AVG(review_quality_score) as avg_quality,
    AVG(helpfulness_ratio) as avg_helpfulness,
    AVG(sentiment_volatility) as avg_volatility,
    AVG(text_emotionality) as avg_emotionality,
    
    -- Business Metrics (Pre-calculated)
    AVG(product_price) as avg_product_price,
    SUM(CASE WHEN rating >= 4.0 THEN 1 ELSE 0 END) as high_rated_reviews,
    ROUND(SUM(CASE WHEN rating >= 4.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as high_rating_percentage,
    
    -- NPS Trends (Pre-calculated)
    ROUND((SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) - SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2) as nps_score
    
FROM v_gold_reviews_complete
WHERE review_year IS NOT NULL
GROUP BY review_year, review_month, main_category, brand, season;
GO

-- Create separate views for time periods
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_historical_recent_dashboard')
BEGIN
    DROP VIEW v_historical_recent_dashboard;
END
GO

CREATE VIEW v_historical_recent_dashboard AS
SELECT 
    main_category,
    brand,
    CASE WHEN review_year < 2020 THEN 'Historical (1996-2019)' ELSE 'Recent (2020-2023)' END as time_period,
    COUNT(*) as total_reviews,
    AVG(avg_rating) as avg_rating,
    AVG(avg_sentiment) as avg_sentiment,
    AVG(avg_quality) as avg_quality,
    SUM(positive_reviews) as total_positive,
    SUM(negative_reviews) as total_negative,
    ROUND(SUM(positive_reviews) * 100.0 / SUM(total_reviews), 2) as positive_percentage,
    ROUND(SUM(negative_reviews) * 100.0 / SUM(total_reviews), 2) as negative_percentage,
    AVG(nps_score) as avg_nps_score
FROM v_time_trends_dashboard
GROUP BY main_category, brand,
         CASE WHEN review_year < 2020 THEN 'Historical (1996-2019)' ELSE 'Recent (2020-2023)' END;
GO

-- Create COVID impact view
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_covid_impact_dashboard')
BEGIN
    DROP VIEW v_covid_impact_dashboard;
END
GO

CREATE VIEW v_covid_impact_dashboard AS
SELECT 
    main_category,
    brand,
    CASE WHEN review_year IN (2020, 2021) THEN 'During-COVID' 
         WHEN review_year < 2020 THEN 'Pre-COVID' 
         ELSE 'Post-COVID' END as covid_period,
    COUNT(*) as total_reviews,
    AVG(avg_rating) as avg_rating,
    AVG(avg_sentiment) as avg_sentiment,
    AVG(avg_quality) as avg_quality,
    SUM(positive_reviews) as total_positive,
    SUM(negative_reviews) as total_negative,
    ROUND(SUM(positive_reviews) * 100.0 / SUM(total_reviews), 2) as positive_percentage,
    ROUND(SUM(negative_reviews) * 100.0 / SUM(total_reviews), 2) as negative_percentage,
    AVG(nps_score) as avg_nps_score
FROM v_time_trends_dashboard
GROUP BY main_category, brand,
         CASE WHEN review_year IN (2020, 2021) THEN 'During-COVID' 
              WHEN review_year < 2020 THEN 'Pre-COVID' 
              ELSE 'Post-COVID' END;
GO

-- Test the time trends dashboard
SELECT TOP 10 
    review_year,
    main_category,
    total_reviews,
    avg_rating,
    positive_percentage,
    nps_score
FROM v_time_trends_dashboard
ORDER BY total_reviews DESC;