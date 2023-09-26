package com.ssafy.topkeyword.topKeyWord.api.service.article;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
public class TopKeywordResponse {
    private Long articleId;
    private String[] topKeywordList;

    @Builder
    public TopKeywordResponse(Long articleId, String[] topKeywordList) {
        this.articleId = articleId;
        this.topKeywordList = topKeywordList;
    }
}
