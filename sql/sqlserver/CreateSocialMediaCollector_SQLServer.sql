USE [master]
GO
/****** Object:  Database [SocialMediaCollector]    Script Date: 09.01.2018 20:09:55 ******/
CREATE DATABASE [SocialMediaCollector]
 CONTAINMENT = NONE
GO
ALTER DATABASE [SocialMediaCollector] SET COMPATIBILITY_LEVEL = 120
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [SocialMediaCollector].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [SocialMediaCollector] SET ANSI_NULL_DEFAULT ON 
GO
ALTER DATABASE [SocialMediaCollector] SET ANSI_NULLS ON 
GO
ALTER DATABASE [SocialMediaCollector] SET ANSI_PADDING ON 
GO
ALTER DATABASE [SocialMediaCollector] SET ANSI_WARNINGS ON 
GO
ALTER DATABASE [SocialMediaCollector] SET ARITHABORT ON 
GO
ALTER DATABASE [SocialMediaCollector] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [SocialMediaCollector] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET CURSOR_DEFAULT  LOCAL 
GO
ALTER DATABASE [SocialMediaCollector] SET CONCAT_NULL_YIELDS_NULL ON 
GO
ALTER DATABASE [SocialMediaCollector] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET QUOTED_IDENTIFIER ON 
GO
ALTER DATABASE [SocialMediaCollector] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET  DISABLE_BROKER 
GO
ALTER DATABASE [SocialMediaCollector] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [SocialMediaCollector] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET RECOVERY FULL 
GO
ALTER DATABASE [SocialMediaCollector] SET  MULTI_USER 
GO
ALTER DATABASE [SocialMediaCollector] SET PAGE_VERIFY NONE  
GO
ALTER DATABASE [SocialMediaCollector] SET DB_CHAINING OFF 
GO
ALTER DATABASE [SocialMediaCollector] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [SocialMediaCollector] SET TARGET_RECOVERY_TIME = 0 SECONDS 
GO
ALTER DATABASE [SocialMediaCollector] SET DELAYED_DURABILITY = DISABLED 
GO
ALTER DATABASE [SocialMediaCollector] SET QUERY_STORE = OFF
GO
USE [SocialMediaCollector]
GO
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET LEGACY_CARDINALITY_ESTIMATION = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET MAXDOP = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET PARAMETER_SNIFFING = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET QUERY_OPTIMIZER_HOTFIXES = PRIMARY;
GO
USE [SocialMediaCollector]
GO
/****** Object:  UserDefinedFunction [dbo].[SplitWords]    Script Date: 09.01.2018 20:09:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/****** Object:  Table [dbo].[FacebookComment]    Script Date: 09.01.2018 20:09:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FacebookComment](
	[Id] [nvarchar](50) NOT NULL,
	[ParentObjectId] [nvarchar](50) NULL,
	[CreatedTime] [datetime] NULL,
	[UpdatedTime] [datetime] NULL,
	[Message] [nvarchar](max) NULL,
	[UserId] [nvarchar](50) NULL,
	[ExtractionDatetime] [datetime] NULL,
	[AttachmentType] [nvarchar](50) NULL,
	[AttachmentTitle] [nvarchar](max) NULL,
	[AttachmentUrl] [nvarchar](500) NULL,
	[ReactionLike] [int] NULL,
	[ReactionLove] [int] NULL,
	[ReactionWow] [int] NULL,
	[ReactionHaha] [int] NULL,
	[ReactionAngry] [int] NULL,
	[ReactionSad] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [dbo].[GET_FACEBOOKCOMMENTS_WITH_POSTID]    Script Date: 09.01.2018 20:09:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[GET_FACEBOOKCOMMENTS_WITH_POSTID]
AS
select distinct
    sub.[Id],
    ISNULL(com.[ParentObjectId],sub.ParentObjectId) as PostId,
    CASE WHEN ISNULL(com.[ParentObjectId],sub.ParentObjectId) = sub.ParentObjectId THEN 'PostComment' ELSE 'Subcomment' END as CommentType,
    sub.[ParentObjectId],
    sub.[CreatedTime],
    sub.[UpdatedTime],
    sub.[Message],
    sub.[UserId],
    sub.[ExtractionDatetime],
    sub.[AttachmentType],
    sub.[AttachmentTitle],
    sub.[AttachmentUrl],
    sub.[ReactionLike],
    sub.[ReactionLove],
    sub.[ReactionWow],
    sub.[ReactionHaha],
    sub.[ReactionAngry],
    sub.[ReactionSad]
 from FacebookComment com
    right join FacebookComment sub on com.Id = sub.ParentObjectId
GO
/****** Object:  Table [dbo].[FacebookUser]    Script Date: 09.01.2018 20:09:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FacebookUser](
	[Id] [nvarchar](50) NOT NULL,
	[Name] [nvarchar](50) NOT NULL,
	[ExtractionDatetime] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[FacebookPost]    Script Date: 09.01.2018 20:09:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FacebookPost](
	[Id] [nvarchar](50) NOT NULL,
	[CreatedTime] [datetime] NULL,
	[UpdatedTime] [datetime] NULL,
	[PostType] [nvarchar](50) NULL,
	[Shares] [int] NULL,
	[Message] [nvarchar](max) NULL,
	[UserId] [nvarchar](50) NULL,
	[ExtractionDatetime] [datetime] NULL,
	[Link] [nvarchar](max) NULL,
	[AttachmentType] [nvarchar](50) NULL,
	[AttachmentTitle] [nvarchar](max) NULL,
	[AttachmentUrl] [nvarchar](500) NULL,
	[ReactionLike] [int] NULL,
	[ReactionLove] [int] NULL,
	[ReactionWow] [int] NULL,
	[ReactionHaha] [int] NULL,
	[ReactionAngry] [int] NULL,
	[ReactionSad] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [dbo].[GET_FACEBOOKPOSTS_FROM]    Script Date: 09.01.2018 20:09:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE view [dbo].[GET_FACEBOOKPOSTS_FROM]
as
select p.*,u.Name from FacebookPost p 
	join FacebookUser u on p.UserId=u.Id
GO
/****** Object:  Table [dbo].[FacebookPage]    Script Date: 09.01.2018 20:09:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FacebookPage](
	[Id] [nvarchar](50) NOT NULL,
	[Name] [nvarchar](100) NOT NULL,
	[Website] [nvarchar](150) NULL,
	[FanCount] [int] NULL,
	[ExtractionDatetime] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[GET_FACEBOOK_COUNT]    Script Date: 09.01.2018 20:09:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE view [dbo].[GET_FACEBOOK_COUNT]
as
select 'Pages' as [TYPE],COUNT(1) as CNT from FacebookPage union
select 'Posts' as [TYPE],count(1) as CNT from FacebookPost union
select 'Users' as [TYPE],count(1) as CNT from FacebookUser union
select 'Comments' as [TYPE],COUNT(1) as CNT from FacebookComment
GO


/****** Object:  Table [dbo].[FacebookCommentCurrent]    Script Date: 09.01.2018 20:09:56 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[FacebookCommentCurrent](
	[Id] [nvarchar](50) NOT NULL,
	[ParentObjectId] [nvarchar](50) NULL,
	[CreatedTime] [datetime] NULL,
	[UpdatedTime] [datetime] NULL,
	[Message] [nvarchar](max) NULL,
	[UserId] [nvarchar](50) NULL,
	[ExtractionDatetime] [datetime] NULL,
	[AttachmentType] [nvarchar](50) NULL,
	[AttachmentTitle] [nvarchar](max) NULL,
	[AttachmentUrl] [nvarchar](500) NULL,
	[ReactionLike] [int] NULL,
	[ReactionLove] [int] NULL,
	[ReactionWow] [int] NULL,
	[ReactionHaha] [int] NULL,
	[ReactionAngry] [int] NULL,
	[ReactionSad] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

/****** Object:  Index [IX_FACEBOOKCOMMENT]    Script Date: 09.01.2018 20:09:57 ******/
CREATE NONCLUSTERED INDEX [IX_FACEBOOKCOMMENT] ON [dbo].[FacebookComment]
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_FACEBOOKPAGE]    Script Date: 09.01.2018 20:09:57 ******/
CREATE NONCLUSTERED INDEX [IX_FACEBOOKPAGE] ON [dbo].[FacebookPage]
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_FACEBOOKPOST]    Script Date: 09.01.2018 20:09:57 ******/
CREATE NONCLUSTERED INDEX [IX_FACEBOOKPOST] ON [dbo].[FacebookPost]
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_FACEBOOKUSER]    Script Date: 09.01.2018 20:09:57 ******/
CREATE NONCLUSTERED INDEX [IX_FACEBOOKUSER] ON [dbo].[FacebookUser]
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [dbo].[FacebookComment] ADD  DEFAULT (getdate()) FOR [ExtractionDatetime]
GO
ALTER TABLE [dbo].[FacebookCommentCurrent] ADD  DEFAULT (getdate()) FOR [ExtractionDatetime]
GO
ALTER TABLE [dbo].[FacebookPage] ADD  DEFAULT (getdate()) FOR [ExtractionDatetime]
GO
ALTER TABLE [dbo].[FacebookPost] ADD  DEFAULT (getdate()) FOR [ExtractionDatetime]
GO
ALTER TABLE [dbo].[FacebookUser] ADD  DEFAULT (getdate()) FOR [ExtractionDatetime]
GO
/****** Object:  StoredProcedure [dbo].[PersistCurrentData]    Script Date: 09.01.2018 20:09:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[PersistCurrentData]
AS
BEGIN
	
	begin tran
	merge FacebookComment tar
	using FacebookCommentCurrent src 
	on (tar.Id=src.Id)
	when matched and (tar.UpdatedTime != src.UpdatedTime or
						tar.Message != src.Message or
						tar.ExtractionDatetime != src.ExtractionDatetime or
						tar.AttachmentType != src.AttachmentType or
						tar.AttachmentTitle != src.AttachmentTitle or
						tar.AttachmentUrl != src.AttachmentUrl or
						tar.ReactionLike != src.ReactionLike or
						tar.ReactionLove != src.ReactionLove or
						tar.ReactionWow != src.ReactionWow or
						tar.ReactionHaha != src.ReactionHaha or 
						tar.ReactionAngry != src.ReactionAngry or
						tar.ReactionSad != src.ReactionSad)
	then update
		set tar.UpdatedTime = src.UpdatedTime,
			tar.Message = src.Message,
			tar.ExtractionDatetime = src.ExtractionDatetime,
			tar.AttachmentType = src.AttachmentType,
			tar.AttachmentTitle = src.AttachmentTitle,
			tar.AttachmentUrl = src.AttachmentUrl,
			tar.ReactionLike = src.ReactionLike,
			tar.ReactionLove = src.ReactionLove,
			tar.ReactionWow = src.ReactionWow,
			tar.ReactionHaha = src.ReactionHaha,
			tar.ReactionAngry = src.ReactionAngry,
			tar.ReactionSad = src.ReactionSad
	when not matched then insert values
		(src.Id,src.ParentObjectId,src.CreatedTime,src.UpdatedTime,src.Message,src.UserId,getdate(),src.AttachmentType,
		src.AttachmentTitle,src.AttachmentUrl,src.ReactionLike,src.ReactionLove,src.ReactionWow,src.ReactionHaha,src.ReactionAngry,src.ReactionSad);

	truncate table FacebookCommentCurrent
	commit transaction

	return 1
END
GO
/****** Object:  StoredProcedure [dbo].[SaveFacebookComment]    Script Date: 09.01.2018 20:09:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[SaveFacebookComment]
    @Id nvarchar(50),
    @ParentObjectId nvarchar(50),
    @CreatedTime datetime,
    @UpdatedTime datetime,
    @Message nvarchar(max),
    @AttachmentType NVARCHAR(50), 
    @AttachmentTitle NVARCHAR(MAX), 
    @AttachmentUrl NVARCHAR(500),
    @ReactionLike INT,
    @ReactionLove INT,
    @ReactionWow INT, 
    @ReactionHaha INT, 
    @ReactionAngry INT, 
    @ReactionSad INT,
    @UserName nvarchar(50),
    @UserId nvarchar(50)
AS
	if(not exists (select null from FacebookUser where id=@UserId))
	begin
		insert into FacebookUser (Id,Name)
		values
		(@UserId,@UserName)
	end

	merge FacebookCommentCurrent tar
	using (select @Id,@ParentObjectId,@CreatedTime,@UpdatedTime,@Message,@UserId,@AttachmentType,@AttachmentTitle,
				  @AttachmentUrl,@ReactionLike,@ReactionLove,@ReactionWow,@ReactionHaha,@ReactionAngry,@ReactionSad) as src 
				  (Id,ParentObjectId,CreatedTime,UpdatedTime,Message,UserId,AttachmentType,AttachmentTitle,
				  AttachmentUrl,ReactionLike,ReactionLove,ReactionWow,ReactionHaha,ReactionAngry,ReactionSad)
	on (tar.Id=src.Id)
	when matched and (tar.UpdatedTime != src.UpdatedTime or
						tar.Message != src.Message or
						tar.AttachmentType != src.AttachmentType or
						tar.AttachmentTitle != src.AttachmentTitle or
						tar.AttachmentUrl != src.AttachmentUrl or
						tar.ReactionLike != src.ReactionLike or
						tar.ReactionLove != src.ReactionLove or
						tar.ReactionWow != src.ReactionWow or
						tar.ReactionHaha != src.ReactionHaha or 
						tar.ReactionAngry != src.ReactionAngry or
						tar.ReactionSad != src.ReactionSad)
	then update
		set tar.UpdatedTime = src.UpdatedTime,
			tar.Message = src.Message,
			tar.ExtractionDatetime = getdate(),
			tar.AttachmentType = src.AttachmentType,
			tar.AttachmentTitle = src.AttachmentTitle,
			tar.AttachmentUrl = src.AttachmentUrl,
			tar.ReactionLike = src.ReactionLike,
			tar.ReactionLove = src.ReactionLove,
			tar.ReactionWow = src.ReactionWow,
			tar.ReactionHaha = src.ReactionHaha,
			tar.ReactionAngry = src.ReactionAngry,
			tar.ReactionSad = src.ReactionSad
	when not matched then insert values
		(src.Id,src.ParentObjectId,src.CreatedTime,src.UpdatedTime,src.Message,src.UserId,getdate(),src.AttachmentType,
		src.AttachmentTitle,src.AttachmentUrl,src.ReactionLike,src.ReactionLove,src.ReactionWow,src.ReactionHaha,src.ReactionAngry,src.ReactionSad);
RETURN 0
GO
/****** Object:  StoredProcedure [dbo].[SaveFacebookPage]    Script Date: 09.01.2018 20:09:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[SaveFacebookPage]
	@Id nvarchar(50),
	@Name nvarchar(100),
	@Website nvarchar(150),
	@FanCount int
AS
	merge FacebookPage tar
	using (select @Id,@Name,@Website,@FanCount) as src (Id,Name,Website,FanCount)
	on (tar.Id=src.Id)
	when matched and (tar.Name != src.Name or
						tar.Website != src.Website or
						tar.FanCount != src.FanCount)
	then update
		set tar.Name = src.Name,
			tar.Website = src.Website,
			tar.FanCount = src.FanCount,
			tar.ExtractionDatetime = getdate()
	when not matched then insert values
		(src.Id,src.Name,src.Website,src.FanCount,getdate());
RETURN 0
GO
/****** Object:  StoredProcedure [dbo].[SaveFacebookPost]    Script Date: 09.01.2018 20:09:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[SaveFacebookPost]
	@Id nvarchar(50),
	@CreatedTime datetime,
	@UpdatedTime datetime,
	@PostType nvarchar(50),
	@Shares int,
	@Message nvarchar(max),
	@Link nvarchar(max),
	@AttachmentType NVARCHAR(50), 
	@AttachmentTitle NVARCHAR(MAX), 
    @AttachmentUrl NVARCHAR(500),
	@ReactionLike INT,
	@ReactionLove INT,
	@ReactionWow INT, 
    @ReactionHaha INT, 
    @ReactionAngry INT, 
    @ReactionSad INT,
	@UserName nvarchar(50),
	@UserId nvarchar(50)
AS
	if(not exists (select null from FacebookUser where id=@UserId))
	begin
		insert into FacebookUser (Id,Name)
		values
		(@UserId,@UserName)
	end
	merge FacebookPost tar
	using (select @Id,@CreatedTime,@UpdatedTime,@PostType,@Shares,@Message,@UserId,@Link,@AttachmentType,@AttachmentTitle,
				  @AttachmentUrl,@ReactionLike,@ReactionLove,@ReactionWow,@ReactionHaha,@ReactionAngry,@ReactionSad) as src
				  (Id,CreatedTime,UpdatedTime,PostType,Shares,Message,UserId,Link,AttachmentType,AttachmentTitle,
				  AttachmentUrl,ReactionLike,ReactionLove,ReactionWow,ReactionHaha,ReactionAngry,ReactionSad)
	on (tar.Id=src.Id)
	when matched and (tar.UpdatedTime != src.UpdatedTime or
						tar.Shares != src.Shares or
						tar.Message != src.Message or
						tar.PostType != src.PostType or 
						tar.Link != src.Link or
						tar.AttachmentType != src.AttachmentType or
						tar.AttachmentTitle != src.AttachmentTitle or
						tar.AttachmentUrl != src.AttachmentUrl or
						tar.ReactionLike != src.ReactionLike or
						tar.ReactionLove != src.ReactionLove or
						tar.ReactionWow != src.ReactionWow or
						tar.ReactionHaha != src.ReactionHaha or 
						tar.ReactionAngry != src.ReactionAngry or
						tar.ReactionSad != src.ReactionSad)
	then update
		set tar.UpdatedTime = src.UpdatedTime,
			tar.Shares = src.Shares,
			tar.Message = src.Message,
			tar.PostType = src.PostType,
			tar.ExtractionDatetime = getdate(),
			tar.Link = src.Link,
			tar.AttachmentType = src.AttachmentType,
			tar.AttachmentTitle = src.AttachmentTitle,
			tar.AttachmentUrl = src.AttachmentUrl,
			tar.ReactionLike = src.ReactionLike,
			tar.ReactionLove = src.ReactionLove,
			tar.ReactionWow = src.ReactionWow,
			tar.ReactionHaha = src.ReactionHaha,
			tar.ReactionAngry = src.ReactionAngry,
			tar.ReactionSad = src.ReactionSad
	when not matched then insert values
		(src.Id,src.CreatedTime,src.UpdatedTime,src.PostType,src.Shares,src.Message,src.UserId,getdate(),src.Link,
		 src.AttachmentType,src.AttachmentTitle,src.AttachmentUrl,src.ReactionLike,src.ReactionLove,src.ReactionWow,src.ReactionHaha,
		 src.ReactionAngry,src.ReactionSad);
RETURN 0
GO
/****** Object:  StoredProcedure [dbo].[SaveFacebookReaction]    Script Date: 09.01.2018 20:09:57 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[SaveFacebookReaction]
	@ObjectId nvarchar(50),
	@ReactionType nvarchar(50),
	@UserId nvarchar(50),
	@UserName nvarchar(50)
AS
	if(not exists (select null from FacebookUser where id=@UserId))
	begin
		insert into FacebookUser (Id,Name)
		values
		(@UserId,@UserName)
	end
	merge FacebookReactionTODAY tar
	using (select @ObjectId, @ReactionType, @UserId) as src (ObjectId, ReactionType, UserId)
	on (tar.objectid=src.ObjectId and tar.userid=src.UserId)
	when matched and tar.ReactionType != src.ReactionType then update
		set tar.ReactionType = src.ReactionType,
			tar.ExtractionDatetime = getdate()
	when not matched then insert values
		(src.ObjectId, src.ReactionType, src.UserId,getdate());
RETURN 0
GO
USE [master]
GO
ALTER DATABASE [SocialMediaCollector] SET  READ_WRITE 
GO
