package com.ssafy.topkeyword.topKeyWord.domain.article;

import com.ssafy.topkeyword.topKeyWord.TimeBaseEntity;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Article extends TimeBaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "article_id")
    private Long id;

    private String title;
    private String subTitle;
    private String writer;
    private LocalDateTime publishedDate;
    private String content;
    private String thumbnailImg;
    private String allKeywords;
    private String topKeywords;
    private Integer hit;

    @Builder
    public Article(String title, String subTitle, String writer, LocalDateTime publishedDate, String content, String thumbnailImg, String allKeywords, String topKeywords, Integer hit) {
        this.title = title;
        this.subTitle = subTitle;
        this.writer = writer;
        this.publishedDate = publishedDate;
        this.content = content;
        this.thumbnailImg = thumbnailImg;
        this.allKeywords = allKeywords;
        this.topKeywords = topKeywords;
        this.hit = hit;
    }

}

