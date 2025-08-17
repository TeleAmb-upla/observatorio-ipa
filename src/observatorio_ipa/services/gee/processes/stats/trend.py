"""GEE Functions to calculate trend statistics for time series image collections

Code is based on https://developers.google.com/earth-engine/tutorials/community/nonparametric-trends
as of 2025-07-26
"""

import ee
from typing import Literal


def _ee_calc_temporal_trend_stats(
    ee_time_series_ic: ee.imagecollection.ImageCollection,
    band_name: str,
    self_join_property: str,  # 'year' or 'month'
    ts_frequency: Literal["years"],
) -> ee.image.Image:
    """Calculates Temporal trend statistics for a given band from a time series image collection.

    Uses Mann-Kendall Non-Parametric Trend Analysis. Currently only support calculations for one band at a time.

    Returns a single image with the following bands:
        - kendall_stat: Mann-Kendall statistic
        - sens_slopes: Sen's slope
        - kendall_variance: Variance of the Mann-Kendall statistic
        - z_score: Z-statistic
        - p_value: P-value of the trend
        - significant_trend: Boolean values indicating slope trends that were statistically significant (p_value <= 0.025)

    The significant trend band is self masked from P values as follows ee_pValue_img.lte(0.025).selfMask()

    Args:
        ee_time_series_ic (ee.imagecollection.ImageCollection): ImageCollection with images for a single month across years
        band_name (str): Name of the band to calculate trend for
        ts_frequency (Literal["years"]): Time series frequency for the trend analysis. Used to calculate the slope

    """

    # Narrow collection to single band
    ee_time_series_ic = ee_time_series_ic.select([band_name])

    # Join the time series to itself
    # lessThan returns images greater than the leftField, counter intuitive.
    # In a series [2000-2002, when the left is '2000' tis would return [2001, 2002]

    primary_key = self_join_property
    ee_after_filter = ee.filter.Filter.lessThan(
        leftField=primary_key, rightField=primary_key
    )
    ee_joined_ic = ee.imagecollection.ImageCollection(
        ee.join.Join.saveAll(matchesKey="after").apply(
            primary=ee_time_series_ic,
            secondary=ee_time_series_ic,
            condition=ee_after_filter,
        )
    )

    # -----------------------------------------------------------------------------------------
    # Mann-Kendall trend statistic
    # #! Removed Clip to basin in this code. Clip will be done elsewhere
    # -----------------------------------------------------------------------------------------

    def _ee_calc_sign(
        ee_current_img: ee.image.Image, ee_after_img: ee.image.Image
    ) -> ee.image.Image:
        """Calculate the sign of the difference between two images.

        The unmask is to prevent accumulation of masked pixels that
        result from the undefined case of when either current or future image
        is masked.  It won't affect the sum, since it's unmasked to zero.
        """
        ee_sign_img = (
            ee_current_img.neq(ee_after_img)  # Zero case
            .multiply(ee_after_img.subtract(ee_current_img).clamp(-1, 1))
            .int()
        )
        return ee_sign_img.unmask(0)

    def _ee_calc_ts_signs(
        ee_current_img: ee.image.Image,
    ) -> ee.imagecollection.ImageCollection:
        ee_after_ic = ee.imagecollection.ImageCollection.fromImages(
            ee_current_img.get("after")
        )
        ee_after_ic = ee_after_ic.map(
            lambda ee_after_img: _ee_calc_sign(ee_current_img, ee_after_img)
        )

        return ee_after_ic

    ee_signs_ic = ee.imagecollection.ImageCollection(
        ee_joined_ic.map(_ee_calc_ts_signs).flatten()
    )
    #! Consider removing clip
    ee_kendall_img = ee_signs_ic.reduce("sum", 2)  # .clip(ee_basin_fc)  # type: ignore
    ee_kendall_img = ee_kendall_img.rename("kendall_stat")

    # -----------------------------------------------------------------------------------------
    # Sen's slope
    #! CAUTION: part of the slope calc was commented out
    # -----------------------------------------------------------------------------------------

    def _ee_calc_slope(
        ee_current_img: ee.image.Image,
        ee_future_img: ee.image.Image,
        ts_frequency: Literal["years"],
    ) -> ee.image.Image:

        return (
            ee_future_img.subtract(ee_current_img)
            # .divide(ee_future_img.date().difference(ee_current_img.date(), ts_frequency))
            .rename("slope").float()
        )

    def _ee_calc_ts_slopes(
        ee_current_img: ee.image.Image, ts_frequency: Literal["years"]
    ) -> ee.imagecollection.ImageCollection:

        ee_after_ic = ee.imagecollection.ImageCollection.fromImages(
            ee_current_img.get("after")
        )

        ee_after_ic = ee_after_ic.map(
            lambda ee_after_img: ee.image.Image(
                _ee_calc_slope(ee_current_img, ee_after_img, ts_frequency)
            )
        )

        return ee_after_ic

    ee_slopes_ic = ee.imagecollection.ImageCollection(
        ee_joined_ic.map(
            lambda ee_current_img: _ee_calc_ts_slopes(
                ee_current_img, ts_frequency=ts_frequency
            )
        ).flatten()
    )

    ee_sensSlope_img = (
        ee_slopes_ic.reduce(ee.reducer.Reducer.median(), 2)
        # .toInt()
        .rename("sens_slopes")
    )

    # -----------------------------------------------------------------------------------------
    # Variance of the Mann-Kendall Statistic
    # -----------------------------------------------------------------------------------------
    """
    # The formula for the Variance of Mann-Kendall requires identifying groups of tied values in a timeseries.
    # The below code does this by first identifying the pixels and values that have ties, then determines the group
    # siz (Number of times the values repeat) using arrays. Then computes the factors for the groups that are used
    # in the variance formula, and finally computes the variance.

    # An simple example of groups for a single pixel in a timeseries:
    # [0.35,0.40,0.40,0.42,0.42,0.42,0.44]
    # - This has two tie groups
    # - One group of two tied values at 0.40 -> t=2
    # - One group of three tied values at 0.42 -> t=3
    """

    def _ee_matches(
        ee_i_img: ee.image.Image, ee_j_img: ee.image.Image
    ) -> ee.image.Image:
        return ee_i_img.eq(ee_j_img)

    def _ee_groups(
        ee_i_img: ee.image.Image, ee_icollection: ee.imagecollection.ImageCollection
    ) -> ee.image.Image:
        """Keep values of pixels that had more than one match in other images.

        >1 because at least one match is the image itself.
        """

        ee_matches_img = ee_icollection.map(
            lambda ee_j_img: _ee_matches(ee_i_img, ee_j_img)
        ).sum()
        return ee_i_img.multiply(ee_matches_img.gt(1))

    # Compute tie group sizes in a sequence.  The first group is discarded.
    def _ee_group_sizes(ee_array_img: ee.image.Image) -> ee.image.Image:
        ee_length_img = ee_array_img.arrayLength(0)
        # Array of indices.  These are 1-indexed.
        ee_indices_img = (
            ee.image.Image([1])
            .arrayRepeat(0, ee_length_img)
            .arrayAccum(0, ee.reducer.Reducer.sum())
            .toArray(1)
        )
        ee_sorted_img = ee_array_img.arraySort()
        ee_left_img = ee_sorted_img.arraySlice(0, 1)
        ee_right_img = ee_sorted_img.arraySlice(0, 0, -1)

        # Indices of the end of runs. Always keep the last index, the end of the sequence.
        ee_mask_img = ee_left_img.neq(ee_right_img).arrayCat(
            ee.image.Image(ee.ee_array.Array([[1]])), 0
        )
        ee_runIndices_img = ee_indices_img.arrayMask(ee_mask_img)

        # Subtract the indices to get run lengths.
        ee_groupSizes_img = ee_runIndices_img.arraySlice(0, 1).subtract(
            ee_runIndices_img.arraySlice(0, 0, -1)
        )
        return ee_groupSizes_img

    # See equation 2.6 in Sen (1968).
    def _ee_factors(ee_image: ee.image.Image) -> ee.image.Image:
        return ee_image.expression("b() * (b() - 1) * (b() * 2 + 5)")

    ee_groups_ic = ee.imagecollection.ImageCollection(
        ee_time_series_ic.map(lambda ee_i_img: _ee_groups(ee_i_img, ee_time_series_ic))
    )
    ee_groupSizes_img = _ee_group_sizes(ee_groups_ic.toArray())
    ee_groupFactors_img = _ee_factors(ee_groupSizes_img)
    ee_groupFactorSum_img = ee_groupFactors_img.arrayReduce("sum", [0]).arrayGet([0, 0])
    ee_count_img = ee_joined_ic.count()
    ee_kendallVariance_img = (
        _ee_factors(ee_count_img).subtract(ee_groupFactorSum_img).divide(18).float()
    )
    ee_kendallVariance_img = ee_kendallVariance_img.rename("kendall_variance")

    # -----------------------------------------------------------------------------------------
    # Significance testing. z-statistic and p-value
    # -----------------------------------------------------------------------------------------

    # compute Z-statistic
    ee_zero_img = ee_kendall_img.multiply(ee_kendall_img.eq(0))
    ee_pos_img = ee_kendall_img.multiply(ee_kendall_img.gt(0)).subtract(1)
    ee_neg_img = ee_kendall_img.multiply(ee_kendall_img.lt(0)).add(1)

    ee_zScore_img = ee_zero_img.add(
        ee_pos_img.divide(ee_kendallVariance_img.sqrt())
    ).add(ee_neg_img.divide(ee_kendallVariance_img.sqrt()))
    ee_zScore_img = ee_zScore_img.rename("z_score")

    # https://en.wikipedia.org/wiki/Error_function#Cumulative_distribution_function
    def _ee_cdf(z):
        return ee.image.Image(0.5).multiply(
            ee.image.Image(1).add(z.divide(ee.image.Image(2).sqrt()).erf())
        )

    # ! Inverse of CDF, not used in this code
    # def _ee_inv_cdf(p: ee.image.Image) -> ee.image.Image:
    #     return ee.image.Image(2).sqrt().multiply(p.multiply(2).subtract(1).erfInv())

    # Compute P-values
    ee_pValue_img = ee.image.Image(1).subtract(_ee_cdf(ee_zScore_img.abs()))
    ee_pValue_img = ee_pValue_img.rename("p_value")

    # Pixels that can have the null hypothesis (there is no trend) rejected.
    # Specifically, if the true trend is zero, there would be less than 5%
    # chance of randomly obtaining the observed result (that there is a trend).
    ee_significant_trend_img = ee_pValue_img.lte(0.025).selfMask()
    ee_significant_trend_img = ee_significant_trend_img.rename("significant_trend")

    ee_trend_stats_img = ee.image.Image(
        [
            ee_kendall_img,
            ee_sensSlope_img,
            ee_kendallVariance_img,
            ee_zScore_img,
            ee_pValue_img,
            ee_significant_trend_img,
        ]
    )

    return ee_trend_stats_img
