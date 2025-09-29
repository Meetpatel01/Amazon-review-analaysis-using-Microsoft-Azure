USE AmazonReviewAnalytics;
GO

-- Sentiment Analysis Dashboard View (Fixed)
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_sentiment_analysis_dashboard')
BEGIN
    DROP VIEW v_sentiment_analysis_dashboard;
END
GO

CREATE VIEW v_sentiment_analysis_dashboard AS
SELECT 
    review_year,
    review_month,
    main_category,
    brand,
    final_sentiment,
    sentiment_intensity,
    
    -- Multi-Model Sentiment Scores (Pre-calculated)
    AVG(vader_compound) as avg_vader_score,
    AVG(textblob_polarity) as avg_textblob_score,
    AVG(rule_based_score) as avg_rule_based_score,
    AVG(ensemble_score) as avg_ensemble_score,
    AVG(sentiment_confidence) as avg_confidence,
    
    -- Sentiment Distribution (Pre-calculated)
    COUNT(*) as total_reviews,
    SUM(CASE WHEN final_sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN final_sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count,
    SUM(CASE WHEN final_sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
    ROUND(SUM(CASE WHEN final_sentiment = 'positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as positive_percentage,
    ROUND(SUM(CASE WHEN final_sentiment = 'negative' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as negative_percentage,
    ROUND(SUM(CASE WHEN final_sentiment = 'neutral' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as neutral_percentage,
    
    -- Advanced Sentiment Features (Pre-calculated)
    AVG(sentiment_volatility) as avg_volatility,
    AVG(text_emotionality) as avg_emotionality,
    AVG(reading_ease) as avg_readability,
    AVG(complexity_score) as avg_complexity,
    AVG(avg_word_length) as avg_word_length,
    STDEV(ensemble_score) as sentiment_std_dev,
    
    -- Sentiment-Rating Alignment (Pre-calculated)
    SUM(CASE WHEN rating_sentiment_alignment = 'aligned' THEN 1 ELSE 0 END) as aligned_reviews,
    SUM(CASE WHEN rating_sentiment_alignment = 'misaligned' THEN 1 ELSE 0 END) as misaligned_reviews,
    ROUND(SUM(CASE WHEN rating_sentiment_alignment = 'aligned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as alignment_percentage,
    
    -- Model Performance (Pre-calculated)
    SUM(CASE WHEN sentiment_confidence < 0.5 THEN 1 ELSE 0 END) as low_confidence_predictions,
    ROUND(SUM(CASE WHEN sentiment_confidence < 0.5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as low_confidence_rate
    
FROM v_gold_reviews_complete
WHERE review_year IS NOT NULL
GROUP BY review_year, review_month, main_category, brand, 
         final_sentiment, sentiment_intensity;
GO

-- Create a separate view for intensity categories
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_sentiment_intensity_dashboard')
BEGIN
    DROP VIEW v_sentiment_intensity_dashboard;
END
GO

CREATE VIEW v_sentiment_intensity_dashboard AS
SELECT 
    review_year,
    review_month,
    main_category,
    brand,
    CASE 
        WHEN ABS(avg_ensemble_score) >= 0.6 THEN 'Strong'
        WHEN ABS(avg_ensemble_score) >= 0.3 THEN 'Moderate'
        ELSE 'Mild'
    END as intensity_category,
    COUNT(*) as total_reviews,
    AVG(avg_ensemble_score) as avg_ensemble_score,
    AVG(avg_confidence) as avg_confidence,
    AVG(avg_volatility) as avg_volatility,
    AVG(avg_emotionality) as avg_emotionality,
    SUM(positive_count) as total_positive,
    SUM(negative_count) as total_negative,
    SUM(neutral_count) as total_neutral,
    ROUND(SUM(positive_count) * 100.0 / SUM(total_reviews), 2) as overall_positive_percentage,
    ROUND(SUM(negative_count) * 100.0 / SUM(total_reviews), 2) as overall_negative_percentage,
    ROUND(SUM(neutral_count) * 100.0 / SUM(total_reviews), 2) as overall_neutral_percentage
FROM v_sentiment_analysis_dashboard
GROUP BY review_year, review_month, main_category, brand,
         CASE 
             WHEN ABS(avg_ensemble_score) >= 0.6 THEN 'Strong'
             WHEN ABS(avg_ensemble_score) >= 0.3 THEN 'Moderate'
             ELSE 'Mild'
         END;
GO

-- Test the sentiment analysis dashboard
SELECT TOP 10 
    review_year,
    main_category,
    final_sentiment,
    total_reviews,
    avg_ensemble_score,
    positive_percentage,
    alignment_percentage
FROM v_sentiment_analysis_dashboard
ORDER BY total_reviews DESC;

-- Test the intensity dashboard
SELECT TOP 10 
    review_year,
    main_category,
    intensity_category,
    total_reviews,
    avg_ensemble_score,
    overall_positive_percentage
FROM v_sentiment_intensity_dashboard
ORDER BY total_reviews DESC;